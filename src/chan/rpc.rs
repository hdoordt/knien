use std::{marker::PhantomData, sync::Arc, task::Poll};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::{Future, Stream, StreamExt};
use lapin::{options::BasicConsumeOptions, BasicProperties};
use pin_project::{pin_project, pinned_drop};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    task::{self, JoinHandle},
};
use uuid::Uuid;

use crate::{delivery_uuid, Connection, Delivery, Result, Bus};

use super::{direct::DirectBus, Channel, Consumer, Publisher};

pub trait RpcBus: DirectBus {
    type ReplyPayload;
}

#[derive(Clone)]
pub struct RpcChannel {
    inner: lapin::Channel,
    pending_replies: Arc<DashMap<Uuid, mpsc::UnboundedSender<lapin::message::Delivery>>>,
}

#[derive(Debug)]
pub struct Reply<B>{
    _marker: B
}

impl<B: RpcBus> Bus for Reply<B> {
    type PublishPayload = B::ReplyPayload;
}

impl <B: RpcBus> DirectBus for Reply<B> {

    type Args = B::Args;

    fn queue(args: Self::Args) -> String {
        B::queue(args)
    }
}

impl <B: RpcBus> RpcBus for Reply<B> {
    type ReplyPayload = B::PublishPayload;
}

impl RpcChannel {
    pub async fn new(connection: &Connection) -> Result<RpcChannel> {
        let chan = connection.inner.create_channel().await?;

        let pending_replies: DashMap<Uuid, mpsc::UnboundedSender<lapin::message::Delivery>> =
            DashMap::new();
        let pending_replies = Arc::new(pending_replies);

        let reply_consumer = chan
            .basic_consume(
                "amq.rabbitmq.reply-to",
                &Uuid::new_v4().to_string(),
                BasicConsumeOptions {
                    // Consuming the direct reply-to queue works only in no-ack mode.
                    // See https://www.rabbitmq.com/direct-reply-to.html#usage
                    no_ack: true,
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;

        let handle_replies: JoinHandle<Result<()>> = task::spawn({
            let mut reply_consumer = reply_consumer;
            let pending_replies = pending_replies.clone();
            async move {
                while let Some(msg_res) = reply_consumer.next().await {
                    match msg_res {
                        Ok(msg) => {
                            // Spawn a task that attempts to forward the reply `msg` that just came in
                            // Getting a lock to pending_replies may block
                            let forward_reply: JoinHandle<()> = task::spawn_blocking({
                                let pending_replies = pending_replies.clone();
                                move || {
                                    let Some(msg_id) = delivery_uuid(&msg) else {
                                            todo!("report abcense of uuid");
                                        };
                                    let msg_id = match msg_id {
                                        Ok(i) => i,
                                        Err(_) => todo!("report error"),
                                    };

                                    if let Some(tx) = pending_replies.get(&msg_id) {
                                        tx.send(msg).unwrap();
                                    } else {
                                        todo!("Report absence of sender");
                                    };
                                }
                            });
                            // `forward_reply` should run to completion
                            drop(forward_reply);
                        }
                        Err(e) => eprintln!("Error receiving reply message: {e:?}"),
                    }
                }
                panic!("Task handle_replies ended");
            }
        });
        // `handle_replies` should run forever
        drop(handle_replies);

        Ok(RpcChannel {
            inner: chan,
            pending_replies,
        })
    }

    pub(crate) fn register_pending_reply(
        &self,
        correlation_uuid: Uuid,
        tx: mpsc::UnboundedSender<lapin::message::Delivery>,
    ) {
        self.pending_replies.insert(correlation_uuid, tx);
    }

    pub(crate) fn remove_pending_reply(&self, correlation_uuid: &Uuid) {
        self.pending_replies.remove(correlation_uuid);
    }

    pub async fn consumer<A, B: RpcBus<Args = A>>(
        &self,
        args: A,
        consumer_tag: &str,
    ) -> Result<Consumer<Self, B>> {
        let queue = B::queue(args);
        self.inner
            .queue_declare(&queue, Default::default(), Default::default())
            .await?;
        let consumer = self
            .inner
            .basic_consume(&queue, consumer_tag, Default::default(), Default::default())
            .await?;

        Ok(Consumer {
            chan: self.clone(),
            inner: consumer,
            _marker: PhantomData,
        })
    }

    pub fn publisher<B: RpcBus>(&self) -> Publisher<Self, B> {
        Publisher {
            chan: self.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl Channel for RpcChannel {
    async fn publish_with_properties(
        &self,
        bytes: &[u8],
        routing_key: &str,
        properties: lapin::BasicProperties,
        correlation_uuid: Uuid,
    ) -> Result<()> {
        let properties = properties.with_correlation_id(correlation_uuid.to_string().into());

        self.inner
            .basic_publish("", routing_key, Default::default(), bytes, properties)
            .await?;

        Ok(())
    }
}

impl<'r, 'p, A, B, P, R> Publisher<RpcChannel, B>
where
    P: Deserialize<'p> + Serialize,
    R: Deserialize<'r> + Serialize,
    B: RpcBus<PublishPayload = P, ReplyPayload = R, Args = A>,
{
    pub async fn publish_recv_many(
        &self,
        args: A,
        payload: &P,
    ) -> Result<impl Stream<Item = Delivery<Reply<B>>>> {
        let correlation_uuid = Uuid::new_v4();
        let (tx, rx) = mpsc::unbounded_channel();

        let rx = ReplyReceiver {
            correlation_uuid,
            inner: rx,
            chan: Some(self.chan.clone()),
            _marker: PhantomData,
        };

        self.chan.register_pending_reply(correlation_uuid, tx);

        let properties = BasicProperties::default().with_reply_to("amq.rabbitmq.reply-to".into());

        self.publish_with_properties(&B::queue(args), payload, properties, correlation_uuid)
            .await?;
        Ok(rx)
    }

    pub async fn publish_recv_one(
        &'r self,
        args: A,
        payload: &P,
    ) -> Result<impl Future<Output = Option<Delivery<Reply<B>>>>> {
        let rx = self.publish_recv_many(args, payload).await?;
        Ok(async { rx.take(1).next().await })
    }
}

#[pin_project(PinnedDrop)]
struct ReplyReceiver<B> {
    #[pin]
    correlation_uuid: Uuid,
    #[pin]
    inner: mpsc::UnboundedReceiver<lapin::message::Delivery>,
    #[pin]
    chan: Option<RpcChannel>,
    _marker: PhantomData<B>,
}

impl<'d, R: Deserialize<'d> + Serialize, B: RpcBus<ReplyPayload = R>> Stream for ReplyReceiver<B> {
    type Item = Delivery<B>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        // Map `lapin::message::Delivery` items to `self::Delivery` items
        this.inner.poll_recv(cx).map(|msg| msg.map(|m| m.into()))
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ReplyReceiver<T> {
    fn drop(self: std::pin::Pin<&mut Self>) {
        let mut this = self.project();
        let chan = this.chan.take().unwrap();
        let correlation_uuid = *this.correlation_uuid;
        task::spawn_blocking(move || chan.remove_pending_reply(&correlation_uuid));
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use futures::StreamExt;
    use serde::{Deserialize, Serialize};
    use tokio::time::timeout;
    use uuid::Uuid;

    use crate::{
        chan::{
            rpc::{RpcBus, RpcChannel},
            tests::{FrameBus, FramePayload, RABBIT_MQ_URL},
            Consumer, Publisher,
        },
        Connection,
    };

    #[derive(Debug, Serialize, Deserialize)]
    pub enum FrameSendError {
        ClientDisconnected,
        Other,
    }

    impl RpcBus for FrameBus {
        type ReplyPayload = Result<(), FrameSendError>;
    }

    #[tokio::test]
    async fn publish_recv_many() -> crate::Result<()> {
        let connection = Connection::connect(RABBIT_MQ_URL).await.unwrap();
        let uuid = Uuid::new_v4();
        tokio::task::spawn({
            let channel = RpcChannel::new(&connection).await.unwrap();
            let mut consumer: Consumer<_, FrameBus> =
                channel.consumer(3, &Uuid::new_v4().to_string()).await?;
            async move {
                let msg = consumer.next().await.unwrap().unwrap();
                msg.ack(false).await.unwrap();
                let payload = msg.get_payload().unwrap();
                assert_eq!(payload.message, uuid.to_string());
                for _ in 0..3 {
                    msg.reply(&Err(FrameSendError::ClientDisconnected), &channel)
                        .await
                        .unwrap();
                }
            }
        });

        let channel = RpcChannel::new(&connection).await.unwrap();
        let publisher: Publisher<_, FrameBus> = channel.publisher();

        let mut rx = publisher
            .publish_recv_many(
                3,
                &FramePayload {
                    message: uuid.to_string(),
                },
            )
            .await
            .unwrap();

        for _ in 0..3 {
            timeout(Duration::from_secs(1), rx.next()).await.unwrap();
        }

        Ok(())
    }

    #[tokio::test]
    async fn publish_recv_one() -> Result<(), crate::Error> {
        let connection = Connection::connect(RABBIT_MQ_URL).await.unwrap();
        let uuid = Uuid::new_v4();
        tokio::task::spawn({
            let channel = RpcChannel::new(&connection).await.unwrap();
            let mut consumer: Consumer<_, FrameBus> =
                channel.consumer(4, &Uuid::new_v4().to_string()).await?;
            async move {
                let msg = consumer.next().await.unwrap().unwrap();
                msg.ack(false).await.unwrap();
                let payload = msg.get_payload().unwrap();
                assert_eq!(payload.message, uuid.to_string());
                msg.reply(&Err(FrameSendError::ClientDisconnected), &channel)
                    .await
                    .unwrap();
            }
        });

        let channel = RpcChannel::new(&connection).await.unwrap();
        let publisher: Publisher<_, FrameBus> = channel.publisher();

        let fut = publisher
            .publish_recv_one(
                4,
                &FramePayload {
                    message: uuid.to_string(),
                },
            )
            .await
            .unwrap();

        timeout(Duration::from_secs(1), fut).await.unwrap();

        Ok(())
    }
}
