use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Bus, Connection, Result};

use super::{Channel, Consumer, Publisher};

pub trait DirectBus: Bus {
    type Args;

    fn queue(args: Self::Args) -> String;
}

#[derive(Clone)]
pub struct DirectChannel {
    inner: lapin::Channel,
}

impl DirectChannel {
    pub async fn new(connection: &Connection) -> Result<Self> {
        let chan = connection.inner.create_channel().await?;

        Ok(Self { inner: chan })
    }

    pub async fn consumer<A, B: DirectBus<Args = A>>(
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

    pub fn publisher<B: DirectBus>(&self) -> Publisher<Self, B> {
        Publisher {
            chan: self.clone(),
            _marker: PhantomData,
        }
    }
}

impl<'p, A, C, B, P> Publisher<C, B>
where
    C: Channel,
    P: Deserialize<'p> + Serialize,
    B: DirectBus<PublishPayload = P, Args = A>,
{
    pub async fn publish(&self, args: A, payload: &P) -> Result<()> {
        let correlation_uuid = Uuid::new_v4();
        self.publish_with_properties(
            &B::queue(args),
            payload,
            Default::default(),
            correlation_uuid,
        )
        .await
    }
}

#[async_trait]
impl Channel for DirectChannel {
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

#[cfg(test)]
pub mod tests {

    use std::time::Duration;

    use crate::{
        chan::{
            direct::DirectChannel,
            tests::{FrameBus, FramePayload, RABBIT_MQ_URL},
            Consumer, Publisher,
        },
        Connection,
    };
    use futures::StreamExt;
    use tokio::{sync::oneshot, time::timeout};
    use uuid::Uuid;

    use super::DirectBus;

    impl DirectBus for FrameBus {
        type Args = u32;

        fn queue(args: Self::Args) -> String {
            format!("frame {}", args)
        }
    }

    #[tokio::test]
    async fn publish() -> crate::Result<()> {
        let connection = Connection::connect(RABBIT_MQ_URL).await.unwrap();
        let uuid = Uuid::new_v4();
        let (tx, rx) = oneshot::channel();
        tokio::task::spawn({
            let channel = DirectChannel::new(&connection).await.unwrap();
            let mut consumer: Consumer<_, FrameBus> =
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
        let publisher: Publisher<_, FrameBus> = channel.publisher();

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
