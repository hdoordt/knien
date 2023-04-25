use std::marker::PhantomData;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use lapin::BasicProperties;
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
pub trait Bus<'p>: Unpin {
    /// The [Channel] this bus is associated with.
    type Chan: Channel;
    /// The type of payload of the messages that are published on or consumed from it.
    type PublishPayload: Serialize + Deserialize<'p>;
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
        reply_uuid: Option<Uuid>,
    ) -> Result<()>;
}

/// A consumer associated with a [Channel] and a [Bus].
/// [Consumer]s implement [futures::Stream], yielding [Delivery]s
/// that are associated with the [Bus].
pub struct Consumer<B> {
    inner: lapin::Consumer,
    _marker: PhantomData<B>,
}

impl<'p, B> Stream for Consumer<B>
where
    B: Bus<'p>,
{
    type Item = Result<Delivery<B>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Adapt any `LapinDelivery`s to `Delivery<T>`
        this.inner
            .poll_next_unpin(cx)
            .map(|m| m.map(|m| m.map(Into::into).map_err(Into::into)))
    }
}

/// A publisher associated with a [Channel] and a [Bus].
/// [Publisher]s allow for publishing messages with payloads
/// of the [Bus::PublishPayload] type. [Publisher]s take care
/// of serializing the payloads before publishing.
#[derive(Clone)]
pub struct Publisher<'p, B: Bus<'p>> {
    chan: B::Chan,
}

impl<'p, B> Publisher<'p, B>
where
    B: Bus<'p>,
{
    async fn publish_with_properties<P>(
        &self,
        routing_key: &str,
        payload: &P,
        properties: BasicProperties,
        correlation_uuid: Uuid,
        reply_uuid: Option<Uuid>,
    ) -> Result<()>
    {
        let bytes = serde_json::to_vec(payload)?;
        self.chan
            .publish_with_properties(
                &bytes,
                routing_key,
                properties,
                correlation_uuid,
                reply_uuid,
            )
            .await
    }
}

#[cfg(test)]
pub use tests::*;

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use lapin::BasicProperties;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use crate::{bus, bus_impl, Channel, Never};

    pub const RABBIT_MQ_URL: &str = "amqp://tg:secret@localhost:5673";

    #[derive(Debug, Serialize, Deserialize)]
    pub struct FramePayload {
        pub message: String,
    }

    #[async_trait]
    impl Channel for Never {
        async fn publish_with_properties(
            &self,
            _payload_bytes: &[u8],
            _routing_key: &str,
            _properties: BasicProperties,
            _correlation_uuid: Uuid,
            _reply_uuid: Option<Uuid>,
        ) -> crate::Result<()> {
            unreachable!()
        }
    }

    bus!("A frame bus", FrameBus);
    bus_impl!(FrameBus, Never, FramePayload);
}

#[doc(hidden)]
#[macro_export]
macro_rules! bus {
    ($doc:literal, $bus:ident) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug)]
        pub enum $bus {}
    };
    ($bus:ident) => {
        $crate::bus!("", $bus);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! bus_impl {
    ($bus:ident, $chan:ty, $publish_payload:ty) => {
        impl<'p> $crate::Bus<'p> for $bus {
            type Chan = $chan;
            type PublishPayload = $publish_payload;
        }
    };
}
