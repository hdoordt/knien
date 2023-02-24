use std::{fmt::Debug, marker::PhantomData, str::FromStr};

use chan::rpc::RpcChannel;

use error::{Error, ReplyError};
use lapin::options::BasicAckOptions;
use serde::{Deserialize, Serialize};

use uuid::Uuid;

pub mod chan;
pub mod error;

pub type Result<T> = std::result::Result<T, Error>;

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

    pub async fn reply(&'p self, reply_payload: &R, chan: &RpcChannel) -> Result<()> {
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

    use crate::{
        chan::{rpc::RpcChannel, Consumer, Publisher},
        Bus, Connection,
    };

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
            let channel = RpcChannel::new(&connection, "".to_owned()).await.unwrap();
            let mut consumer: Consumer<_, FrameBus> = channel.consumer("consumer").await?;
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

        let channel = RpcChannel::new(&connection, "".to_owned()).await.unwrap();
        let publisher: Publisher<_, FrameBus> = channel.publisher();

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
            let channel = RpcChannel::new(&connection, "".to_owned()).await.unwrap();
            let mut consumer: Consumer<_, FrameBus> = channel.consumer("consumer").await?;
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

        let channel = RpcChannel::new(&connection, "".to_owned()).await.unwrap();
        let publisher: Publisher<_, FrameBus> = channel.publisher();

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
