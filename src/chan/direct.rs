use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Connection, Result, Bus};

use super::{Consumer, Publisher, Channel};


pub trait DirectBus: Bus {
    const QUEUE: &'static str;
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

    pub async fn consumer<B: DirectBus>(&self, consumer_tag: &str) -> Result<Consumer<Self, B>> {
        self.inner
            .queue_declare(B::QUEUE, Default::default(), Default::default())
            .await?;
        let consumer = self
            .inner
            .basic_consume(
                B::QUEUE,
                consumer_tag,
                Default::default(),
                Default::default(),
            )
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

impl<'p, C, B, P> Publisher<C, B>
where
    C: Channel,
    P: Deserialize<'p> + Serialize,
    B: DirectBus<PublishPayload = P>,
{
    pub async fn publish(&self, payload: &P) -> Result<()> {
        let correlation_uuid = Uuid::new_v4();
        self.publish_with_properties(B::QUEUE, payload, Default::default(), correlation_uuid)
            .await
    }
}