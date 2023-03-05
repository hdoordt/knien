use std::fmt::Display;
use std::iter::empty;
use std::iter::once;
use std::iter::Empty;
use std::marker::PhantomData;
use std::ops::Add;
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
            .basic_consume(
                &routing_key.key,
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

struct Initial;
struct NonEmpty;

pub struct RoutingKeyBuilder<B, I, S> {
    split: Split<'static, &'static str>,
    iter: I,
    _state: PhantomData<S>,
    _marker: PhantomData<B>,
}

impl<B: TopicBus> RoutingKeyBuilder<B, Empty<&'static str>, Initial> {
    pub fn new() -> Self {
        Self {
            split: B::TOPIC.split("."),
            iter: empty(),
            _state: PhantomData,
            _marker: PhantomData,
        }
    }
}

impl<B: TopicBus, I: Iterator<Item = &'static str>, S> RoutingKeyBuilder<B, I, S> {
    pub fn finish(self) -> RoutingKey<B> {
        let RoutingKeyBuilder { iter, _marker, .. } = self;
        let key = String::from_iter(iter);
        RoutingKey { key, _marker }
    }
}

macro_rules! impl_routing_key_builder {
    ($state:ty, $delim:expr) => {
        impl<B: TopicBus, I: Iterator<Item = &'static str>> RoutingKeyBuilder<B, I, $state> {
            pub fn word(
                self,
            ) -> RoutingKeyBuilder<B, impl Iterator<Item = &'static str>, NonEmpty> {
                let RoutingKeyBuilder {
                    mut split,
                    iter,
                    _marker,
                    ..
                } = self;
                let iter = iter.chain($delim).chain(split.next());
                RoutingKeyBuilder {
                    split,
                    iter,
                    _state: PhantomData,
                    _marker,
                }
            }

            pub fn star(
                self,
            ) -> RoutingKeyBuilder<B, impl Iterator<Item = &'static str>, NonEmpty> {
                let RoutingKeyBuilder {
                    mut split,
                    iter,
                    _marker,
                    ..
                } = self;
                let iter = iter.chain($delim).chain(once("*"));
                split.next();

                RoutingKeyBuilder {
                    split,
                    iter,
                    _state: PhantomData,
                    _marker,
                }
            }

            pub fn hash(
                self,
            ) -> RoutingKeyBuilder<B, impl Iterator<Item = &'static str>, NonEmpty> {
                let RoutingKeyBuilder {
                    mut split,
                    iter,
                    _marker,
                    ..
                } = self;
                let iter = iter.chain($delim).chain(once("#"));
                split.next();

                RoutingKeyBuilder {
                    split,
                    iter,
                    _state: PhantomData,
                    _marker,
                }
            }
        }

        impl<B: TopicBus + 'static, I: Iterator<Item = &'static str> + 'static> Add<&'static str>
            for RoutingKeyBuilder<B, I, $state>
        {
            type Output = RoutingKeyBuilder<B, Box<dyn Iterator<Item = &'static str>>, NonEmpty>;

            fn add(self, rhs: &'static str) -> Self::Output {
                match rhs {
                    "*" => {
                        let RoutingKeyBuilder {
                            split,
                            iter,
                            _marker,
                            ..
                        } = self.star();
                        RoutingKeyBuilder {
                            split,
                            // TODO: Not a big fan of this allocation
                            iter: Box::new(iter),
                            _state: PhantomData,
                            _marker,
                        }
                    }
                    "#" => {
                        let RoutingKeyBuilder {
                            split,
                            iter,
                            _marker,
                            ..
                        } = self.hash();
                        RoutingKeyBuilder {
                            split,
                            // TODO: Not a big fan of this allocation
                            iter: Box::new(iter),
                            _state: PhantomData,
                            _marker,
                        }
                    }
                    word => {
                        let RoutingKeyBuilder {
                            mut split,
                            iter,
                            _marker,
                            ..
                        } = self;
                        let next = split.next().unwrap();
                        assert_eq!(
                            next, word,
                            "Invalid routing key part. Expected {next}, got {word}"
                        );
                        let iter = iter.chain($delim).chain(once(next));
                        RoutingKeyBuilder {
                            split,
                            iter: Box::new(iter),
                            _state: PhantomData,
                            _marker,
                        }
                    }
                }
            }
        }
    };
}

impl_routing_key_builder!(Initial, empty());
impl_routing_key_builder!(NonEmpty, once("."));

#[cfg(test)]
mod tests {
    use crate::{chan::topic::TopicBus, Bus};

    use super::RoutingKeyBuilder;

    struct MyTopic;
    impl Bus for MyTopic {
        type PublishPayload = ();
    }
    impl TopicBus for MyTopic {
        const TOPIC: &'static str = "this.is.a.topic";
    }

    #[test]
    fn test_routing_key_builder() {
        let builder: RoutingKeyBuilder<MyTopic, _, _> = RoutingKeyBuilder::new();
        assert_eq!(builder.finish().key, "");

        let builder: RoutingKeyBuilder<MyTopic, _, _> = RoutingKeyBuilder::new();
        assert_eq!(builder.word().finish().key, "this");

        let builder: RoutingKeyBuilder<MyTopic, _, _> = RoutingKeyBuilder::new();
        assert_eq!(builder.word().star().finish().key, "this.*");

        let builder: RoutingKeyBuilder<MyTopic, _, _> = RoutingKeyBuilder::new();
        assert_eq!(builder.word().star().word().finish().key, "this.*.a");

        let builder: RoutingKeyBuilder<MyTopic, _, _> = RoutingKeyBuilder::new();
        assert_eq!(
            builder.word().star().word().hash().finish().key,
            "this.*.a.#"
        );

        let builder: RoutingKeyBuilder<MyTopic, _, _> = RoutingKeyBuilder::new();
        let builder = builder + "this" + "*" + "a" + "#";
        assert_eq!(builder.finish().key, "this.*.a.#");

        let builder: RoutingKeyBuilder<MyTopic, _, _> = RoutingKeyBuilder::new();
        let builder = builder + "this" + "is" + "a" + "topic";
        assert_eq!(builder.finish().key, "this.is.a.topic");
    }
}
