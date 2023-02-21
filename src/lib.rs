use std::{fmt::Debug, marker::PhantomData, str::FromStr, sync::Arc, task::Poll};

use dashmap::DashMap;
use futures::{Future, Stream, StreamExt};
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions},
    BasicProperties,
};
use pin_project::{pin_project, pinned_drop};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    task::{self, JoinHandle},
};
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Mq(lapin::Error),
    Serde(serde_json::Error),
    Uuid(uuid::Error),
    Reply(ReplyError),
}

impl From<lapin::Error> for Error {
    fn from(e: lapin::Error) -> Self {
        Self::Mq(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Self {
        Self::Uuid(e)
    }
}

#[derive(Debug)]
pub enum ReplyError {
    NoCorrelationUuid,
    NoReplyToConfigured,
}

pub trait Bus {
    const QUEUE: &'static str;
    type PublishPayload;
    type ReplyPayload;
}

pub struct Connection {
    inner: lapin::Connection,
}

impl Connection {
    pub async fn connect(mq_url: &str) -> Result<Self> {
        let connection = lapin::Connection::connect(mq_url, Default::default()).await?;
        Ok(Self { inner: connection })
    }
}

#[derive(Clone)]
pub struct Channel {
    inner: lapin::Channel,
    pending_replies: Arc<DashMap<Uuid, mpsc::UnboundedSender<lapin::message::Delivery>>>,
    exchange: String,
}

impl Channel {
    pub async fn new(connection: &Connection, exchange: String) -> Result<Channel> {
        let chan = connection.inner.create_channel().await?;
        if exchange != "" {
            chan.exchange_declare(
                exchange.as_ref(),
                lapin::ExchangeKind::Topic,
                Default::default(),
                Default::default(),
            )
            .await?;
        }
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

        Ok(Channel {
            inner: chan,
            pending_replies,
            exchange,
        })
    }

    async fn publish_with_properties(
        &self,
        bytes: &[u8],
        routing_key: &str,
        properties: BasicProperties,
        correlation_uuid: Uuid,
    ) -> Result<()> {
        let properties = properties.with_correlation_id(correlation_uuid.to_string().into());

        self.inner
            .basic_publish(
                self.exchange.as_ref(),
                routing_key,
                Default::default(),
                &bytes,
                properties,
            )
            .await?;

        Ok(())
    }

    pub async fn consumer<B: Bus>(&self, consumer_tag: &str) -> Result<Consumer<B>> {
        self.inner
            .queue_declare(B::QUEUE, Default::default(), Default::default())
            .await?;
        let consumer = self
            .inner
            .basic_consume(
                B::QUEUE,
                consumer_tag,
                Default::default(),
                Default::default(),
            )
            .await?;

        Ok(Consumer {
            chan: self.clone(),
            inner: consumer,
            _marker: PhantomData,
        })
    }

    pub fn publisher<B>(&self) -> Publisher<B> {
        Publisher {
            chan: self.clone(),
            _marker: PhantomData,
        }
    }

    fn register_pending_reply(
        &self,
        correlation_uuid: Uuid,
        tx: mpsc::UnboundedSender<lapin::message::Delivery>,
    ) {
        self.pending_replies.insert(correlation_uuid, tx);
    }

    fn remove_pending_reply(&self, correlation_uuid: &Uuid) {
        self.pending_replies.remove(correlation_uuid);
    }
}

#[derive(Debug)]
pub struct Delivery<B> {
    inner: lapin::message::Delivery,
    _marker: PhantomData<B>,
}

impl<'p, 'r, B, P, R> Delivery<B>
where
    P: Deserialize<'p> + Serialize + Debug,
    R: Deserialize<'r> + Serialize,
    B: Bus<PublishPayload = P, ReplyPayload = R>,
{
    pub fn get_payload(&'p self) -> Result<P> {
        Ok(serde_json::from_slice(&self.inner.data)?)
    }

    pub fn get_uuid(&self) -> Option<Result<Uuid>> {
        delivery_uuid(&self.inner)
    }

    pub async fn reply(&'p self, reply_payload: &R, chan: &Channel) -> Result<()> {
        let Some(correlation_uuid) = self.get_uuid() else {
            return Err(Error::Reply(ReplyError::NoCorrelationUuid));
        };
        let Some(reply_to) = self.inner.properties.reply_to().as_ref().map(|r | r.as_str()) else {
            return Err(Error::Reply(ReplyError::NoReplyToConfigured))
        };

        let correlation_uuid = correlation_uuid?;

        let bytes = serde_json::to_vec(reply_payload)?;
        chan.publish_with_properties(&bytes, reply_to, Default::default(), correlation_uuid)
            .await
    }

    pub async fn ack(&self, multiple: bool) -> Result<()> {
        self.inner.ack(BasicAckOptions { multiple }).await?;
        Ok(())
    }
}

impl<B> From<lapin::message::Delivery> for Delivery<B> {
    fn from(delivery: lapin::message::Delivery) -> Self {
        Self {
            inner: delivery,
            _marker: PhantomData,
        }
    }
}

#[pin_project]
pub struct Consumer<B> {
    chan: Channel,
    #[pin]
    inner: lapin::Consumer,
    _marker: PhantomData<B>,
}

impl<'p, B, P> Stream for Consumer<B>
where
    P: Deserialize<'p> + Serialize,
    B: Bus<PublishPayload = P>,
{
    type Item = Result<Delivery<B>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        // Adapt any `LapinDelivery`s to `Delivery<T>`
        this.inner
            .poll_next(cx)
            .map(|m| m.map(|m| m.map(Into::into).map_err(Into::into)))
    }
}

pub struct Publisher<B> {
    chan: Channel,
    _marker: PhantomData<B>,
}

impl<'p, B, P> Publisher<B>
where
    P: Deserialize<'p> + Serialize,
    B: Bus<PublishPayload = P>,
{
    pub async fn publish(&self, payload: &P) -> Result<()> {
        let correlation_uuid = Uuid::new_v4();
        self.publish_with_properties(payload, BasicProperties::default(), correlation_uuid)
            .await
    }

    async fn publish_with_properties(
        &self,
        payload: &P,
        properties: BasicProperties,
        correlation_uuid: Uuid,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(payload)?;
        self.chan
            .publish_with_properties(&bytes, B::QUEUE, properties, correlation_uuid)
            .await
    }
}

impl<'r, 'p, B, P, R> Publisher<B>
where
    P: Deserialize<'p> + Serialize,
    R: Deserialize<'r> + Serialize,
    B: Bus<PublishPayload = P, ReplyPayload = R>,
{
    pub async fn publish_recv_many(&self, payload: &P) -> Result<impl Stream<Item = Delivery<R>>> {
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

        self.publish_with_properties(payload, properties, correlation_uuid)
            .await?;
        Ok(rx)
    }

    pub async fn publish_recv_one(
        &'r self,
        payload: &P,
    ) -> Result<impl Future<Output = Option<Delivery<R>>>> {
        let rx = self.publish_recv_many(payload).await?;
        Ok(async { rx.take(1).next().await })
    }
}

#[pin_project(PinnedDrop)]
struct ReplyReceiver<T> {
    #[pin]
    correlation_uuid: Uuid,
    #[pin]
    inner: mpsc::UnboundedReceiver<lapin::message::Delivery>,
    #[pin]
    chan: Option<Channel>,
    _marker: PhantomData<T>,
}

impl<'d, T: Deserialize<'d> + Serialize> Stream for ReplyReceiver<T> {
    type Item = Delivery<T>;

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
        let correlation_uuid = this.correlation_uuid.clone();
        task::spawn_blocking(move || chan.remove_pending_reply(&correlation_uuid));
    }
}

fn delivery_uuid(delivery: &lapin::message::Delivery) -> Option<Result<Uuid>> {
    let Some(correlation_id) = delivery.properties.correlation_id() else {
        return None;
    };
    Some(Uuid::from_str(correlation_id.as_str()).map_err(Into::into))
}

#[cfg(test)]
mod tests {
    const RABBIT_MQ_URL: &str = "amqp://tg:secret@localhost:5672";
    use std::time::Duration;

    use futures::StreamExt;
    use serde::{Deserialize, Serialize};
    use tokio::time::timeout;
    use uuid::Uuid;

    use crate::{Bus, Channel, Connection, Consumer, Publisher};

    #[derive(Debug, Serialize, Deserialize)]
    struct FramePayload {
        message: String,
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum FrameSendError {
        ClientDisconnected,
        Other,
    }

    struct FrameBus;

    impl Bus for FrameBus {
        const QUEUE: &'static str = "frame";

        type PublishPayload = FramePayload;

        type ReplyPayload = Result<(), FrameSendError>;
    }

    #[tokio::test]
    async fn publish_recv_many() -> Result<(), crate::Error> {
        let connection = Connection::connect(RABBIT_MQ_URL).await.unwrap();
        let uuid = Uuid::new_v4();
        tokio::task::spawn({
            let channel = Channel::new(&connection, "".to_owned()).await.unwrap();
            let mut consumer: Consumer<FrameBus> = channel.consumer("consumer").await?;
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

        let channel = Channel::new(&connection, "".to_owned()).await.unwrap();
        let publisher: Publisher<FrameBus> = channel.publisher();

        let mut rx = publisher
            .publish_recv_many(&FramePayload {
                message: uuid.to_string(),
            })
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
            let channel = Channel::new(&connection, "".to_owned()).await.unwrap();
            let mut consumer: Consumer<FrameBus> = channel.consumer("consumer").await?;
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

        let channel = Channel::new(&connection, "".to_owned()).await.unwrap();
        let publisher: Publisher<FrameBus> = channel.publisher();

        let fut = publisher
            .publish_recv_one(&FramePayload {
                message: uuid.to_string(),
            })
            .await
            .unwrap();

        timeout(Duration::from_secs(1), fut).await.unwrap();

        Ok(())
    }
}
