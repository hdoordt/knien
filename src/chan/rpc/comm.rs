use std::marker::PhantomData;

use futures::Stream;
use lapin::BasicProperties;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    error::Error, Bus, Channel, Delivery, DirectBus, Publisher, ReplyError, Result, RpcChannel,
};

use super::ReplyReceiver;

pub trait RpcCommBus {
    type Args;

    fn queue(args: Self::Args) -> String;

    type InitialPayload;

    type BackPayload;
    type ForthPayload;
}

impl<B: RpcCommBus> Bus for B {
    type PublishPayload = B::BackPayload;
}

impl<B: RpcCommBus> DirectBus for B {
    type Args = B::Args;

    fn queue(args: Self::Args) -> String {
        B::queue(args)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Never {}

#[derive(Debug)]
pub struct CommReply<B> {
    _marker: PhantomData<B>,
}

impl<B: RpcCommBus> RpcCommBus for CommReply<B> {
    type Args = B::Args;

    fn queue(args: Self::Args) -> String {
        B::queue(args)
    }

    type InitialPayload = Never;

    type BackPayload = B::ForthPayload;

    type ForthPayload = B::BackPayload;
}

impl RpcChannel {
    pub fn comm_publisher<B: RpcCommBus>(&self) -> Publisher<Self, B> {
        Publisher {
            chan: self.clone(),
            _marker: PhantomData,
        }
    }
}

impl<'r, 'p, B> Publisher<RpcChannel, B>
where
    B::InitialPayload: Deserialize<'r> + Serialize,
    B::BackPayload: Deserialize<'p> + Serialize,
    B::ForthPayload: Deserialize<'r> + Serialize,
    B: RpcCommBus,
{
    pub async fn publish_recv_comm(
        &self,
        args: B::Args,
        payload: &B::InitialPayload,
    ) -> Result<impl Stream<Item = Delivery<B>>> {
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

        self.publish_with_properties(&B::queue(args), payload, properties, correlation_uuid)
            .await?;
        Ok(rx)
    }
}

impl<'i, 'b, 'f, B> Delivery<B>
where
    B: RpcCommBus,
    B::BackPayload: Deserialize<'b> + Serialize,
    B::ForthPayload: Deserialize<'f> + Serialize,
{
    pub async fn reply_forth(
        &self,
        back_payload: &B::ForthPayload,
        chan: &impl Channel,
    ) -> Result<()> {
        let Some(correlation_uuid) = self.get_uuid() else {
            return Err(Error::Reply(ReplyError::NoCorrelationUuid));
        };
        let Some(reply_to) = self.inner.properties.reply_to().as_ref().map(|r | r.as_str()) else {
            return Err(Error::Reply(ReplyError::NoReplyToConfigured))
        };

        let correlation_uuid = correlation_uuid?;

        let bytes = serde_json::to_vec(back_payload)?;

        chan.publish_with_properties(&bytes, reply_to, Default::default(), correlation_uuid)
            .await
    }

    pub async fn reply_recv_many(
        &self,
        back_payload: &B::BackPayload,
        rpc_chan: &RpcChannel,
    ) -> Result<impl Stream<Item = Delivery<CommReply<B>>>> {
        let Some(correlation_uuid) = self.get_uuid() else {
            return Err(Error::Reply(ReplyError::NoCorrelationUuid));
        };
        let Some(reply_to) = self.inner.properties.reply_to().as_ref().map(|r | r.as_str()) else {
            return Err(Error::Reply(ReplyError::NoReplyToConfigured))
        };

        let bytes = serde_json::to_vec(back_payload)?;
        let correlation_uuid = correlation_uuid?;

        let (tx, rx) = mpsc::unbounded_channel();

        let rx = ReplyReceiver {
            correlation_uuid,
            inner: rx,
            chan: Some(rpc_chan.clone()),
            _marker: PhantomData,
        };

        rpc_chan.register_pending_reply(correlation_uuid, tx);

        let properties = BasicProperties::default().with_reply_to("amq.rabbitmq.reply-to".into());
        rpc_chan
            .publish_with_properties(&bytes, reply_to, properties, correlation_uuid)
            .await?;
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use crate::{chan::tests::FramePayload, rpc::tests::FrameSendError, rpc_comm_bus};

    rpc_comm_bus!(FrameCommBus, FramePayload, FramePayload, Result<(), FrameSendError>, u32, |args| format!(
        "frame_comm_{}",
        args
    ));
}

#[macro_export]
macro_rules! rpc_comm_bus {
    ($bus:ident, $initial_payload:ty, $back_payload:ty, $forth_payload:ty, $args:ty, $queue:expr) => {
        #[derive(Debug)]
        pub enum $bus {}

        impl $crate::RpcCommBus for $bus {
            type InitialPayload = $initial_payload;
            type BackPayload = $back_payload;
            type ForthPayload = $forth_payload;
            type Args = $args;

            fn queue(args: Self::Args) -> String {
                ($queue)(args)
            }
        }
    };
}
