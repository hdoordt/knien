use std::{marker::PhantomData, task::Poll};

use async_trait::async_trait;
use futures::{Stream, Future, StreamExt};
use lapin::BasicProperties;
use pin_project::{pin_project, pinned_drop};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task};
use uuid::Uuid;

use crate::{Bus, Delivery, Result};

use self::rpc::RpcChannel;

pub mod rpc;
pub mod topic;

#[async_trait]
pub trait Channel: Clone {
    async fn publish_with_properties(
        &self,
        bytes: &[u8],
        routing_key: &str,
        properties: BasicProperties,
        correlation_uuid: Uuid,
    ) -> Result<()>;
}


#[pin_project]
pub struct Consumer<C, B> {
    chan: C,
    #[pin]
    inner: lapin::Consumer,
    _marker: PhantomData<B>,
}

impl<'p, C, B, P> Stream for Consumer<C, B>
where
    C: Channel,
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

pub struct Publisher<C, B> {
    chan: C,
    _marker: PhantomData<B>,
}

impl<'p, C, B, P> Publisher<C, B>
where
    C: Channel,
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

impl<'r, 'p, B, P, R> Publisher<RpcChannel, B>
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
    chan: Option<RpcChannel>,
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
        let correlation_uuid = *this.correlation_uuid;
        task::spawn_blocking(move || chan.remove_pending_reply(&correlation_uuid));
    }
}
