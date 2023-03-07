use std::marker::PhantomData;

use async_trait::async_trait;
use futures::Stream;
use lapin::BasicProperties;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Delivery, Result};

pub mod direct;
pub mod rpc;
pub mod topic;

pub use direct::*;
pub use rpc::*;
pub use topic::*;

pub trait Bus {
    type PublishPayload;
}

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

impl<'p, C, B> Stream for Consumer<C, B>
where
    C: Channel,
    B: Bus,
    B::PublishPayload: Deserialize<'p> + Serialize,
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

impl<C, B> Publisher<C, B>
where
    C: Channel,
{
    async fn publish_with_properties<'p, P>(
        &self,
        routing_key: &str,
        payload: &P,
        properties: BasicProperties,
        correlation_uuid: Uuid,
    ) -> Result<()>
    where
        P: Deserialize<'p> + Serialize,
    {
        let bytes = serde_json::to_vec(payload)?;
        self.chan
            .publish_with_properties(&bytes, routing_key, properties, correlation_uuid)
            .await
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::bus;

    pub const RABBIT_MQ_URL: &str = "amqp://tg:secret@localhost:5673";

    #[derive(Debug, Serialize, Deserialize)]
    pub struct FramePayload {
        pub message: String,
    }

    bus!(FrameBus, FramePayload);
}

#[macro_export]
macro_rules! bus {
    ($name:ident, $publish_payload:ty) => {
        #[derive(Debug)]
        pub enum $name {}

        impl $crate::Bus for $name {
            type PublishPayload = $publish_payload;
        }
    };
}
