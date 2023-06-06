#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
use std::{fmt::Debug, marker::PhantomData, str::FromStr};

use lapin::options::{BasicAckOptions, BasicNackOptions};
use serde::{Deserialize, Serialize};

use tracing::{debug, trace};
use uuid::Uuid;

mod chan;
mod error;

pub use chan::*;
pub use error::*;

/// Alias for a Result with the error type [Error].
pub type Result<T> = std::result::Result<T, Error>;

/// A connection to the RabbitMQ broker
pub struct Connection {
    inner: lapin::Connection,
}

impl Connection {
    /// Make a new connection to RabbitMQ
    pub async fn connect(mq_url: &str) -> Result<Self> {
        let connection = lapin::Connection::connect(mq_url, Default::default()).await?;
        debug!("Connected to RabbitMQ instance");
        Ok(Self { inner: connection })
    }
}

#[derive(Debug)]
/// A message that contains a payload associated with a bus
pub struct Delivery<B> {
    inner: lapin::message::Delivery,
    _marker: PhantomData<B>,
}

impl<'p, 'r, B> Delivery<B>
where
    B: Bus,
    B::PublishPayload: Deserialize<'p> + Serialize,
{
    /// Deserialize and return the payload from the [Delivery]
    pub fn get_payload(&'p self) -> Result<B::PublishPayload> {
        Ok(serde_json::from_slice(&self.inner.data)?)
    }

    /// Get the message correlation [Uuid]
    pub fn get_uuid(&self) -> Option<Result<Uuid>> {
        delivery_uuid(&self.inner, 0)
    }

    /// Get the correlation [Uuid] of the message this message
    /// is a reply to
    pub fn get_reply_uuid(&self) -> Option<Result<Uuid>> {
        delivery_uuid(&self.inner, 1)
    }

    /// Get routing key of this message
    pub fn routing_key(&self) -> &str {
        self.inner.routing_key.as_str()
    }

    /// Ack the message
    pub async fn ack(&self, multiple: bool) -> Result<()> {
        self.inner.ack(BasicAckOptions { multiple }).await?;
        if let Some(Ok(uuid)) = self.get_uuid() {
            trace!("Acked message with correlation UUID {uuid}");
        }
        Ok(())
    }

    /// Nack the message
    pub async fn nack(&self, multiple: bool, requeue: bool) -> Result<()> {
        self.inner
            .nack(BasicNackOptions { multiple, requeue })
            .await?;
        if let Some(Ok(uuid)) = self.get_uuid() {
            trace!("Nacked message with correlation UUID {uuid}");
        }
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

fn delivery_uuid(delivery: &lapin::message::Delivery, index: usize) -> Option<Result<Uuid>> {
    let Some(correlation_id) = delivery.properties.correlation_id() else {
        return None;
    };
    let mut parts = correlation_id.as_str().split(':');
    parts
        .nth(index)
        .map(|uuid| Uuid::from_str(uuid).map_err(Into::into))
}

fn fmt_correlation_id(correlation_uuid: Uuid, reply_uuid: Option<Uuid>) -> String {
    format!(
        "{correlation_uuid}:{}",
        reply_uuid.map(|r| r.to_string()).unwrap_or_default()
    )
}
