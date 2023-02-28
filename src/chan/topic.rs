use std::fmt::Display;
use std::marker::PhantomData;
use std::str::Split;

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

    pub async fn consumer<B: TopicBus>(
        &self,
        routing_key: RoutingKey<B>,
        consumer_tag: &str,
    ) -> Result<Consumer<Self, B>> {
        self.inner
            .queue_declare(&routing_key.key, Default::default(), Default::default())
            .await?;
        let consumer = self
            .inner
            .basic_consume(&routing_key.key, consumer_tag, Default::default(), Default::default())
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

/// A Routing key that can be used to consume messages from a [TopicBus].
/// See [RoutingKeyBuilder].
#[derive(Debug)]
pub struct RoutingKey<B> {
    key: String,
    _marker: PhantomData<B>,
}

impl<B> Display for RoutingKey<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.key.fmt(f)
    }
}

/// A builder for [RoutingKey]s. Enforces built routing keys
/// are valid for the topic defined in the [TopicBus] this
/// [RoutingKeyBuilder] is constructed for
pub struct RoutingKeyBuilder<B> {
    iter: Split<'static, &'static str>,
    key: String,
    _marker: PhantomData<B>,
}

impl<B: TopicBus> RoutingKeyBuilder<B> {
    /// Create a new [RoutingKeyBuilder] for the [TopicBus]
    pub fn new() -> Self {
        let iter = B::TOPIC.split(".");
        Self {
            iter,
            key: String::new(),
            _marker: PhantomData,
        }
    }

    /// Add the next word to the routing key
    pub fn word(self) -> Self {
        let RoutingKeyBuilder {
            mut iter,
            key: mut repr,
            _marker,
        } = self;
        if !repr.is_empty() {
            repr.push('.');
        }
        repr.push_str(iter.next().expect("No more parts left in topic"));
        Self {
            iter,
            key: repr,
            _marker,
        }
    }

    /// Add a star (`*`) to the routing key
    pub fn star(self) -> Self {
        let RoutingKeyBuilder {
            mut iter,
            key: mut repr,
            _marker,
        } = self;

        if !repr.is_empty() {
            repr.push('.');
        }
        repr.push('*');
        let _ = iter.next().expect("No more parts left in topic");
        Self {
            iter,
            key: repr,
            _marker,
        }
    }

    /// Add a hash (`*`) to the routing key, finishing the [RoutingKey]
    pub fn hash(mut self) -> RoutingKey<B> {
        if !self.key.is_empty() {
            self.key.push('.');
        }
        self.key.push('#');
        self.finish()
    }

    /// Finish the [RoutingKey]
    pub fn finish(self) -> RoutingKey<B> {
        RoutingKey {
            key: self.key,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        chan::topic::{RoutingKeyBuilder, TopicBus},
        Bus,
    };

    #[test]
    fn test_routing_key_builder() {
        struct MyTopic;
        impl Bus for MyTopic {
            type PublishPayload = ();
        }
        impl TopicBus for MyTopic {
            const TOPIC: &'static str = "this.is.a.topic";
        }

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(builder.finish().key, "");

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(builder.word().finish().key, "this");

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(builder.word().star().finish().key, "this.*");

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(builder.word().star().word().finish().key, "this.*.a");

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(builder.word().star().word().hash().key, "this.*.a.#");
    }
}
