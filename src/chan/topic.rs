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

    pub async fn consumer<B: TopicBus, T: AsRef<str>>(
        &self,
        routing_key: RoutingKey<T, B>,
        consumer_tag: &str,
    ) -> Result<Consumer<Self, B>> {
        let topic = routing_key.repr.as_ref();
        self.inner
            .queue_declare(topic, Default::default(), Default::default())
            .await?;
        let consumer = self
            .inner
            .basic_consume(topic, consumer_tag, Default::default(), Default::default())
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


pub struct RoutingKey<T, B> {
    repr: T,
    _marker: PhantomData<B>,
}

pub struct RoutingKeyBuilder<B> {
    iter: Split<'static, &'static str>,
    repr: String,
    _marker: PhantomData<B>,
}

impl<B: TopicBus> RoutingKeyBuilder<B> {
    /// Create a new [TopicBuilder]
    pub fn new() -> Self {
        let iter = B::TOPIC.split(".");
        Self {
            iter,
            repr: String::new(),
            _marker: PhantomData,
        }
    }

    /// Add the next word to the routing key
    pub fn word(self) -> Self {
        let RoutingKeyBuilder {
            mut iter,
            mut repr,
            _marker,
        } = self;
        if !repr.is_empty() {
            repr.push('.');
        }
        repr.push_str(iter.next().expect("No more parts left in topic"));
        Self {
            iter,
            repr,
            _marker,
        }
    }

    pub fn star(self) -> Self {
        let RoutingKeyBuilder {
            mut iter,
            mut repr,
            _marker,
        } = self;

        if !repr.is_empty() {
            repr.push('.');
        }
        repr.push('*');
        let _ = iter.next().expect("No more parts left in topic");
        Self {
            iter,
            repr,
            _marker,
        }
    }

    pub fn hash(self) -> RoutingKey<String, B> {
        let mut repr = self.repr;
        if !repr.is_empty() {
            repr.push('.');
        }
        repr.push('#');
        RoutingKey {
            repr,
            _marker: PhantomData,
        }
    }

    pub fn finish(self) -> RoutingKey<String, B> {
        RoutingKey {
            repr: self.repr,
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
    fn test_topic_builder() {
        struct MyTopic;
        impl Bus for MyTopic {
            type PublishPayload = ();
        }
        impl TopicBus for MyTopic {
            const TOPIC: &'static str = "this.is.a.cool.topic";
        }

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(builder.finish().repr, "");

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(builder.word().finish().repr, "this");

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(builder.word().star().finish().repr, "this.*");

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(
            builder.word().star().word().finish().repr,
            "this.*.a"
        );

        let builder: RoutingKeyBuilder<MyTopic> = RoutingKeyBuilder::new();
        assert_eq!(
            builder.word().star().word().hash().repr,
            "this.*.a.#"
        );
    }
}
