use std::{fmt::Debug, marker::PhantomData, str::FromStr};

use error::Error;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use serde::{Deserialize, Serialize};

use uuid::Uuid;

pub mod chan;
pub mod error;

pub use chan::*;
pub use error::*;

pub type Result<T> = std::result::Result<T, Error>;

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

impl<'p, 'r, B> Delivery<B>
where
    B: Bus,
    B::PublishPayload: Deserialize<'p> + Serialize,
{
    pub fn get_payload(&'p self) -> Result<B::PublishPayload> {
        Ok(serde_json::from_slice(&self.inner.data)?)
    }

    pub fn get_uuid(&self) -> Option<Result<Uuid>> {
        delivery_uuid(&self.inner)
    }

    pub async fn ack(&self, multiple: bool) -> Result<()> {
        self.inner.ack(BasicAckOptions { multiple }).await?;
        Ok(())
    }

    pub async fn nack(&self, multiple: bool, requeue: bool) -> Result<()> {
        self.inner
            .nack(BasicNackOptions { multiple, requeue })
            .await?;
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
