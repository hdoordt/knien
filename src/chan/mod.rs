use std::marker::PhantomData;

use async_trait::async_trait;
use futures::Stream;
use lapin::BasicProperties;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Delivery, Result};

mod direct;
#[cfg(feature = "rpc")]
mod rpc;
#[cfg(feature = "topic")]
mod topic;

pub use direct::*;
#[cfg(feature = "rpc")]
pub use rpc::*;
#[cfg(feature = "topic")]
pub use topic::*;

/// A Bus. Base trait for several other buses
pub trait Bus {
    /// The type of payload of the messages that are published on or consumed from it.
    type PublishPayload;
}

#[async_trait]
/// A RrabbitMQ channel
pub trait Channel: Clone {
    /// Publish a message onto a queue
    async fn publish_with_properties(
        &self,
        payload_bytes: &[u8],
        routing_key: &str,
        properties: BasicProperties,
        correlation_uuid: Uuid,
    ) -> Result<()>;
}

/// A consumer associated with a [Channel] and a [Bus].
/// [Consumer]s implement [futures::Stream], yielding [Delivery]s
/// that are associated with the [Bus].
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

/// A publisher associated with a [Channel] and a [Bus].
/// [Publisher]s allow for publishing messages with payloads
/// of the [Bus::PublishPayload] type. [Publisher]s take care
/// of serializing the payloads before publishing.
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
pub use tests::*;

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::bus;

    pub const RABBIT_MQ_URL: &str = "amqp://tg:secret@localhost:5673";

    #[derive(Debug, Serialize, Deserialize)]
    pub struct FramePayload {
        pub message: String,
    }

    bus!("A frame bus", FrameBus, FramePayload);
}

#[doc(hidden)]
#[macro_export]
macro_rules! bus {
    ($doc:literal, $name:ident, $publish_payload:ty) => {
        #[doc = $doc]
        #[derive(Debug)]
        pub enum $name {}

        impl $crate::Bus for $name {
            type PublishPayload = $publish_payload;
        }
    };
    ($name:ident, $publish_payload:ty) => {
        $crate::bus!("", $name, $publish_payload);
    };
}
