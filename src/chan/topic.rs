use std::marker::PhantomData;

use async_trait::async_trait;
use uuid::Uuid;

use crate::Bus;
use crate::Connection;
use crate::Result;

use super::Channel;
use super::Consumer;
use super::Publisher;

pub trait TopicBus: Bus {
    const TOPIC: &'static str;
}

#[derive(Clone)]
pub struct TopicChannel {
    inner: lapin::Channel,
    exchange: String,
}

impl TopicChannel {
    pub async fn new(connection: &Connection, exchange: String) -> Result<Self> {
        let chan = connection.inner.create_channel().await?;
        chan.exchange_declare(
            exchange.as_ref(),
            lapin::ExchangeKind::Topic,
            Default::default(),
            Default::default(),
        )
        .await?;

        Ok(Self {
            inner: chan,
            exchange,
        })
    }

    pub async fn consumer<B: TopicBus>(&self, consumer_tag: &str) -> Result<Consumer<Self, B>> {
        let topic = todo!();
        self.inner
            .queue_declare(topic, Default::default(), Default::default())
            .await?;
        let consumer = self
            .inner
            .basic_consume(
                topic,
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

    pub fn publisher<B: TopicBus>(&self) -> Publisher<Self, B> {
        Publisher {
            chan: self.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl Channel for TopicChannel {
    async fn publish_with_properties(
        &self,
        bytes: &[u8],
        routing_key: &str,
        properties: lapin::BasicProperties,
        correlation_uuid: Uuid,
    ) -> Result<()> {
        let properties = properties.with_correlation_id(correlation_uuid.to_string().into());

        self.inner
            .basic_publish(
                self.exchange.as_ref(),
                routing_key,
                Default::default(),
                bytes,
                properties,
            )
            .await?;

        Ok(())
    }
}
