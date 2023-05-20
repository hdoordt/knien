use std::{any::type_name, marker::PhantomData};

use futures::{Future, Stream, StreamExt};
use lapin::BasicProperties;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::debug;
use uuid::Uuid;

use crate::{
    error::Error, Bus, Channel, Consumer, Delivery, DirectBus, Publisher, ReplyError, Result,
    RpcBus, RpcChannel,
};

use super::ReplyReceiver;

/// An RPC-based communication bus that defines an `InitialPayload`,
/// a `BackPayload` and `ForthPayload`, and supports flows where a single
/// initial message is sent, after which back-and-forth a communication
/// over which multiple messages can be sent is setup.
pub trait RpcCommBus: Unpin {
    /// The arguments used to format the queue
    type Args;

    /// Method with which queue names are built
    fn queue(args: Self::Args) -> String;

    /// The type of payload of the message that is initally sent onto the bus
    type InitialPayload;

    /// The type of payload of the messages that the receiver of the initial message sends back
    type BackPayload;
    /// The type of payload of the messages that the initiator of the communication sends forth
    type ForthPayload;
}

impl<B: RpcCommBus> Bus for B {
    type Chan = RpcChannel;
    type PublishPayload = B::InitialPayload;
}

impl<B: RpcCommBus> DirectBus for B {
    type Args = B::Args;

    fn queue(args: Self::Args) -> String {
        B::queue(args)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[doc(hidden)]
pub enum Never {}

#[derive(Debug)]
/// A reply on an [RpcCommBus].
pub struct BackReply<B> {
    _marker: PhantomData<B>,
}

impl<B: RpcCommBus> Bus for BackReply<B> {
    type Chan = RpcChannel;
    type PublishPayload = B::BackPayload;
}

impl<B: RpcCommBus> DirectBus for BackReply<B> {
    type Args = B::Args;

    fn queue(args: Self::Args) -> String {
        B::queue(args)
    }
}

impl<B: RpcCommBus> RpcBus for BackReply<B> {
    type ReplyPayload = B::ForthPayload;
}

#[derive(Debug)]
/// A reply on an [RpcCommBus].
pub struct ForthReply<B> {
    _marker: PhantomData<B>,
}

impl<B: RpcCommBus> Bus for ForthReply<B> {
    type Chan = RpcChannel;
    type PublishPayload = B::ForthPayload;
}

impl<B: RpcCommBus> DirectBus for ForthReply<B> {
    type Args = B::Args;

    fn queue(args: Self::Args) -> String {
        B::queue(args)
    }
}

impl RpcChannel {
    /// Create a new [Consumer] for the [RpcCommBus] that declares
    /// a direct queue with the name produced by [RpcCommBus::queue]
    /// given the passed [RpcCommBus::Args]
    pub async fn comm_consumer<B: RpcCommBus>(
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
            "Created consumer for RPC comm bus {} for queue {queue} with consumer tag {consumer_tag}",
            type_name::<B>()
        );

        Ok(Consumer {
            inner: consumer,
            _marker: PhantomData,
        })
    }

    /// Setup a new [Publisher] associated wit the [RpcCommBus].
    pub fn comm_publisher<B: RpcCommBus>(&self) -> Publisher<B> {
        debug!("Created comm publisher for RPC bus {}", type_name::<B>());
        Publisher { chan: self.clone() }
    }
}

impl<'i, 'b, 'f, B> Publisher<B>
where
    B: RpcCommBus,
    B::InitialPayload: Deserialize<'i> + Serialize,
    B::BackPayload: Deserialize<'b> + Serialize,
    B::ForthPayload: Deserialize<'f> + Serialize,
{
    /// Publish an initial message onto the [RpcCommBus], and receive the replies with
    /// with [RpcCommBus::BackPayload] payloads onto the returned [Stream].
    /// The replies can be obtained by calling [futures::StreamExt::next] on the returned [Stream].
    pub async fn publish_recv_comm(
        &self,
        args: B::Args,
        payload: &B::InitialPayload,
    ) -> Result<impl Stream<Item = Delivery<BackReply<B>>>> {
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
        debug!("Publishing message with correlation UUID {correlation_uuid}, setting up back-and-forth communication");
        self.publish_with_properties(&B::queue(args), payload, properties, correlation_uuid, None)
            .await?;
        Ok(rx)
    }
}

impl<'i, 'b, 'f, B> Delivery<B>
where
    B: RpcCommBus,
    B::InitialPayload: Deserialize<'i> + Serialize,
    B::BackPayload: Deserialize<'b> + Serialize,
    B::ForthPayload: Deserialize<'f> + Serialize,
{
    /// Reply to a message that was sent by the receiver of the initial message.
    pub async fn reply_forth(
        &self,
        forth_payload: &B::ForthPayload,
        chan: &impl Channel,
    ) -> Result<()> {
        let Some(correlation_uuid) = self.get_uuid() else {
            return Err(Error::Reply(ReplyError::NoCorrelationUuid));
        };
        let Some(reply_to) = self.inner.properties.reply_to().as_ref().map(|r | r.as_str()) else {
            return Err(Error::Reply(ReplyError::NoReplyToConfigured))
        };

        let reply_uuid = correlation_uuid?;

        let bytes = serde_json::to_vec(forth_payload)?;

        debug!("Replying forth to message with correlation UUID {reply_uuid}");
        let correlation_uuid = Uuid::new_v4();
        chan.publish_with_properties(
            &bytes,
            reply_to,
            Default::default(),
            correlation_uuid,
            Some(reply_uuid),
        )
        .await
    }

    /// Reply to a message that was sent by the receiver of the initial message and await
    /// multiple replies from the receiver.
    /// The messages can be obtained by calling [futures::StreamExt::next] on the returned [Stream].
    pub async fn reply_recv_many(
        &self,
        back_payload: &B::BackPayload,
        rpc_chan: &RpcChannel,
    ) -> Result<impl Stream<Item = Delivery<ForthReply<B>>>> {
        let Some(correlation_uuid) = self.get_uuid() else {
            return Err(Error::Reply(ReplyError::NoCorrelationUuid));
        };
        let Some(reply_to) = self.inner.properties.reply_to().as_ref().map(|r | r.as_str()) else {
            return Err(Error::Reply(ReplyError::NoReplyToConfigured))
        };

        let bytes = serde_json::to_vec(back_payload)?;
        let reply_uuid = correlation_uuid?;

        let (tx, rx) = mpsc::unbounded_channel();

        let rx = ReplyReceiver {
            correlation_uuid: reply_uuid,
            inner: rx,
            chan: Some(rpc_chan.clone()),
            _marker: PhantomData,
        };

        rpc_chan.register_pending_reply(reply_uuid, tx);

        let properties = BasicProperties::default().with_reply_to("amq.rabbitmq.reply-to".into());

        debug!("Replying back to message with correlation UUID {reply_uuid}");
        let correlation_uuid = Uuid::new_v4();
        rpc_chan
            .publish_with_properties(
                &bytes,
                reply_to,
                properties,
                correlation_uuid,
                Some(reply_uuid),
            )
            .await?;
        Ok(rx)
    }

    /// Reply to a message that was sent by the receiver of the initial message and await
    /// one reply from the receiver.
    /// The message can be obtained by `await`ing the returned [Future].
    pub async fn reply_recv(
        &self,
        back_payload: &B::BackPayload,
        rpc_chan: &RpcChannel,
    ) -> Result<impl Future<Output = Delivery<ForthReply<B>>>> {
        let rx = self.reply_recv_many(back_payload, rpc_chan).await?;
        Ok(async move {
            // As `rx` won't be dropped, we can assume that either
            // this future never resolves, or it resolves to a `Some`
            rx.take(1).next().await.unwrap()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::time::timeout;
    use tracing::info;
    use uuid::Uuid;

    use crate::{
        chan::tests::FramePayload, rpc_comm_bus, setup_test_logging, Connection, Consumer,
        FrameSendError, Publisher, RpcChannel, RABBIT_MQ_URL,
    };

    rpc_comm_bus!(FrameCommBus, FramePayload, FramePayload, Result<(), FrameSendError>, u32, |args| format!(
        "frame_comm_{}",
        args
    ));

    #[tokio::test]
    async fn publish_recv_comm() -> crate::Result<()> {
        let _log = setup_test_logging();
        let connection = Connection::connect(RABBIT_MQ_URL).await?;
        let uuid = Uuid::new_v4();

        let channel = RpcChannel::new(&connection).await?;
        let publisher: Publisher<FrameCommBus> = channel.comm_publisher();

        let handle = tokio::task::spawn({
            let channel = RpcChannel::new(&connection).await?;
            let mut consumer: Consumer<FrameCommBus> = channel
                .comm_consumer(123, &Uuid::new_v4().to_string())
                .await?;

            async move {
                let initial = consumer.next().await.unwrap().unwrap();
                info!("Got initial");
                for i in 0..3 {
                    info!("Sending back reply {i}");
                    let response = initial
                        .reply_recv(
                            &FramePayload {
                                message: i.to_string(),
                            },
                            &channel,
                        )
                        .await
                        .unwrap()
                        .await;
                    info!("Got forth reply {i}");
                    assert_eq!(response.get_payload().unwrap(), Ok(()))
                }
            }
        });

        let mut rx = publisher
            .publish_recv_comm(
                123,
                &FramePayload {
                    message: uuid.to_string(),
                },
            )
            .await?;
        info!("Sent initial");

        for i in 0..3 {
            let back_reply = timeout(Duration::from_secs(5), rx.next())
                .await
                .unwrap()
                .unwrap();
            info!("Got back reply {i}");
            back_reply.reply(&Ok(()), &channel).await?;
        }
        handle.await.unwrap();
        Ok(())
    }
}

#[macro_export]
/// Declare a new [RpcCommBus].
macro_rules! rpc_comm_bus {
    ($doc:literal, $bus:ident, $initial_payload:ty, $back_payload:ty, $forth_payload:ty, $args:ty, $queue:expr) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug)]
        pub enum $bus {}

        $crate::rpc_comm_bus_impl!(
            $bus,
            $initial_payload,
            $back_payload,
            $forth_payload,
            $args,
            $queue
        );
    };
    (doc = $doc:literal, bus = $bus:ident, initial = $initial_payload:ty, back = $back_payload:ty, forth = $forth_payload:ty, args = $args:ty, queue = $queue:expr) => {
        $crate::rpc_comm_bus!(
            $doc,
            $bus,
            $initial_payload,
            $back_payload,
            $forth_payload,
            $args,
            $queue
        );
    };
    ($bus:ident, $initial_payload:ty, $back_payload:ty, $forth_payload:ty, $args:ty, $queue:expr) => {
        $crate::rpc_comm_bus!(
            "",
            $bus,
            $initial_payload,
            $back_payload,
            $forth_payload,
            $args,
            $queue
        );
    };
    (bus = $bus:ident, initial = $initial_payload:ty, back = $back_payload:ty, forth = $forth_payload:ty, args = $args:ty, queue = $queue:expr) => {
        $crate::rpc_comm_bus!(
            $bus,
            $initial_payload,
            $back_payload,
            $forth_payload,
            $args,
            $queue
        );
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! rpc_comm_bus_impl {
    ($bus:ty, $initial_payload:ty, $back_payload:ty, $forth_payload:ty, $args:ty, $queue:expr) => {
        impl $crate::RpcCommBus for $bus {
            type InitialPayload = $initial_payload;
            type BackPayload = $back_payload;
            type ForthPayload = $forth_payload;
            type Args = $args;

            fn queue(args: Self::Args) -> String {
                #[allow(clippy::redundant_closure_call)]
                ($queue)(args)
            }
        }
    };
}
