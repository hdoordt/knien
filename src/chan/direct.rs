use std::{any::type_name, marker::PhantomData};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

use crate::{fmt_correlation_id, Bus, Connection, Result};

use super::{Channel, Consumer, Publisher};

/// A bus that allows publishing on a direct queue.
pub trait DirectBus: Bus {
    /// The arguments used to format the queue
    type Args;

    /// Method with which queue names are built
    fn queue(args: Self::Args) -> String;
}

#[derive(Clone)]
/// A channel for publishing messages on direct queues.
pub struct DirectChannel {
    inner: lapin::Channel,
}

impl DirectChannel {
    /// Create a new [DirectChannel]
    pub async fn new(connection: &Connection) -> Result<Self> {
        let chan = connection.inner.create_channel().await?;

        Ok(Self { inner: chan })
    }

    /// Create a new [Consumer] for the [DirectBus] that declares
    /// a direct queue with the name produced by [DirectBus::queue]
    /// given the passed [DirectBus::Args]
    pub async fn consumer<B: DirectBus>(
        &self,
        args: B::Args,
        consumer_tag: &str,
    ) -> Result<Consumer<B>> {
        let queue = B::queue(args);
        self.inner
            .queue_declare(&queue, Default::default(), Default::default())
            .await?;
        let consumer = self
            .inner
            .basic_consume(&queue, consumer_tag, Default::default(), Default::default())
            .await?;

        debug!(
            "Created consumer for direct bus {} for queue {queue} with consumer tag {consumer_tag}",
            type_name::<B>()
        );

        Ok(Consumer {
            inner: consumer,
            _marker: PhantomData,
        })
    }

    /// Create a new [Publisher] that allows for publishing on the [DirectBus]
    pub fn publisher<B: DirectBus<Chan = DirectChannel>>(&self) -> Publisher<B> {
        debug!("Created publisher for direct bus {}", type_name::<B>());
        Publisher { chan: self.clone() }
    }
}

impl<'p, B> Publisher<B>
where
    B: DirectBus,
    B::PublishPayload: Deserialize<'p> + Serialize,
{
    /// Publish a message onto a direct queue with the name produced by [DirectBus::queue]
    /// given the passed [DirectBus::Args]
    pub async fn publish(&self, args: B::Args, payload: &B::PublishPayload) -> Result<()> {
        let correlation_uuid = Uuid::new_v4();
        self.publish_with_properties(
            &B::queue(args),
            payload,
            Default::default(),
            correlation_uuid,
            None,
        )
        .await
    }
}

#[async_trait]
impl Channel for DirectChannel {
    async fn publish_with_properties(
        &self,
        payload_bytes: &[u8],
        routing_key: &str,
        properties: lapin::BasicProperties,
        correlation_uuid: Uuid,
        reply_uuid: Option<Uuid>,
    ) -> Result<()> {
        let correlation_id = fmt_correlation_id(correlation_uuid, reply_uuid);
        let properties = properties.with_correlation_id(correlation_id.into());

        debug!("Publishing message with correlation UUID {correlation_uuid} a direct channel with routing key {routing_key}");
        self.inner
            .basic_publish(
                "",
                routing_key,
                Default::default(),
                payload_bytes,
                properties,
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {

    use std::time::Duration;

    use futures::StreamExt;
    use tokio::{sync::oneshot, time::timeout};
    use uuid::Uuid;

    use crate::{
        chan::tests::{FramePayload, RABBIT_MQ_URL},
        direct_bus, Connection, Consumer, DirectChannel, Publisher,
    };

    direct_bus!(
        doc = "A Bus onto which frames are sent",
        bus = FrameBus,
        publish = FramePayload,
        args = u32,
        queue = |args| format!("frame_{}", args),
        serialize = serde_json::to_vec,
        deserialize = serde_json::from_slice
    );

    #[tokio::test]
    async fn publish() -> crate::Result<()> {
        let connection = Connection::connect(RABBIT_MQ_URL).await.unwrap();
        let uuid = Uuid::new_v4();
        let (tx, rx) = oneshot::channel();
        tokio::task::spawn({
            let channel = DirectChannel::new(&connection).await.unwrap();
            let mut consumer: Consumer<FrameBus> =
                channel.consumer(3, &Uuid::new_v4().to_string()).await?;
            async move {
                let msg = consumer.next().await.unwrap().unwrap();
                msg.ack(false).await.unwrap();
                let payload = msg.get_payload().unwrap();
                assert_eq!(payload.message, uuid.to_string());
                tx.send(()).unwrap();
            }
        });

        let channel = DirectChannel::new(&connection).await.unwrap();
        let publisher: Publisher<FrameBus> = channel.publisher();

        publisher
            .publish(
                3,
                &FramePayload {
                    message: uuid.to_string(),
                },
            )
            .await
            .unwrap();

        timeout(Duration::from_secs(3), rx).await.unwrap().unwrap();

        Ok(())
    }
}

/// Declare a new [DirectBus].
#[macro_export]
macro_rules! direct_bus {
    ($doc:literal, $bus:ident, $publish_payload:ty, $args:ty, $queue:expr, $serialize:expr, $deserialize:expr) => {
        $crate::bus!($doc, $bus);

        $crate::bus_impl!($bus, $crate::DirectChannel, $publish_payload, $serialize, $deserialize);

        $crate::direct_bus_impl!($bus, $args, $queue);
    };
    (doc = $doc:literal, bus = $bus:ident, publish = $publish_payload:ty, args = $args:ty, queue = $queue:expr, serialize = $serialize:expr, deserialize = $deserialize:expr) => {
        $crate::direct_bus!($doc, $bus, $publish_payload, $args, $queue, $serialize, $deserialize);
    };
    ($bus:ident, $publish_payload:ty, $args:ty, $queue:expr, $serialize:expr, $deserialize:expr) => {
        $crate::direct_bus!("", $bus, $publish_payload, $args, $queue, $serialize, $deserialize);
    };
    (bus = $bus:ident, publish = $publish_payload:ty, args = $args:ty, queue = $queue:expr, serialize = $serialize:expr, deserialize = $deserialize:expr) => {
        $crate::direct_bus!($bus, $publish_payload, $args, $queue, $serialize, $deserialize);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! direct_bus_impl {
    ($bus:ident, $args:ty, $queue:expr) => {
        impl $crate::DirectBus for $bus {
            type Args = $args;

            fn queue(args: Self::Args) -> String {
                #[allow(clippy::redundant_closure_call)]
                ($queue)(args)
            }
        }
    };
}
