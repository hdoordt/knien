use std::{marker::PhantomData, str::FromStr, sync::Arc, task::Poll};

use dashmap::DashMap;
use futures::Stream;
use lapin::BasicProperties;
use pin_project::{pin_project, pinned_drop};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task};
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
        chan.exchange_declare(
            exchange.as_ref(),
            lapin::ExchangeKind::Topic,
            Default::default(),
            Default::default(),
        )
        .await?;

        let pending_replies = DashMap::new();
        let pending_replies = Arc::new(pending_replies);

        todo!("setup reply forwarding task");

        Ok(Channel {
            inner: chan,
            pending_replies,
            exchange,
        })
    }

    pub async fn consumer<B: Bus>(&self, consumer_tag: &str) -> Result<Consumer<B>> {
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

pub struct Delivery<B> {
    inner: lapin::message::Delivery,
    _marker: PhantomData<B>,
}

impl<'p, 'r, B, P, R> Delivery<B>
where
    P: Deserialize<'p> + Serialize,
    R: Deserialize<'r> + Serialize,
    B: Bus<PublishPayload = P, ReplyPayload = R>,
{
    pub fn get_payload(&'p self) -> Result<P> {
        Ok(serde_json::from_slice(&self.inner.data)?)
    }

    pub fn get_uuid(&self) -> Option<Result<Uuid>> {
        let Some(correlation_id) = self.inner.properties.correlation_id() else {
            return None;
        };
        Some(Uuid::from_str(correlation_id.as_str()).map_err(Into::into))
    }

    pub async fn reply(&self, reply_payload: &R, publisher: &Publisher<B>) -> Result<()> {
        let Some(correlation_uuid) = self.get_uuid() else {
            return Err(Error::Reply(ReplyError::NoCorrelationUuid));
        };

        let Some(reply_to) = self.inner.properties.reply_to().as_ref().map(|r | r.as_str()) else {
            return Err(Error::Reply(ReplyError::NoReplyToConfigured))
        };

        let correlation_uuid = correlation_uuid?;

        todo!("publish reply")
        
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
        let properties = properties.with_correlation_id(correlation_uuid.to_string().into());

        self.chan
            .inner
            .basic_publish(
                self.chan.exchange.as_ref(),
                B::QUEUE,
                Default::default(),
                &bytes,
                properties,
            )
            .await?;

        Ok(())
    }
}

impl<'p, B, P, R> Publisher<B>
where
    P: Deserialize<'p> + Serialize,
    R: Deserialize<'p> + Serialize,
    B: Bus<PublishPayload = P, ReplyPayload = R>,
{
    pub async fn publish_recv_many(&self, payload: &P) -> Result<ReplyReceiver<R>> {
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
        task::spawn_blocking({
            let chan = self.chan.take().unwrap();
            let correlation_uuid = self.correlation_uuid;

            move || chan.remove_pending_reply(&correlation_uuid)
        });
    }
}
#[cfg(test)]
mod tests {
    const RABBIT_MQ_URL: &str = "amqp://tg:secret@localhost:5672";
    use futures::StreamExt;
    use serde::{Deserialize, Serialize};

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
    async fn it_works() -> Result<(), crate::Error> {
        let connection = Connection::connect(RABBIT_MQ_URL).await.unwrap();

        tokio::task::spawn({
            let channel = Channel::new(&connection, "".to_owned()).await.unwrap();
            let mut consumer: Consumer<FrameBus> = channel.consumer("consumer").await?;
            let publisher: Publisher<FrameBus> = channel.publisher();
            async move {
                let msg = consumer.next().await.unwrap().unwrap();
                let payload = msg.get_payload().unwrap();
                dbg!(payload);
                msg.reply(&Err(FrameSendError::ClientDisconnected), &publisher)
                    .await
                    .unwrap();
            }
        });

        let channel = Channel::new(&connection, "".to_owned()).await.unwrap();
        let publisher: Publisher<FrameBus> = channel.publisher();

        publisher
            .publish(&FramePayload {
                message: "hello, world!".to_owned(),
            })
            .await
            .unwrap();

        Ok(())
    }
}
