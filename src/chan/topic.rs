use std::fmt::Display;
use std::marker::PhantomData;

use async_trait::async_trait;
use regex::Regex;
use uuid::Uuid;

use crate::Bus;
use crate::Connection;
use crate::Result;

use super::Channel;
use super::Consumer;
use super::Publisher;

pub trait Exchange: Clone + Send + Sync {
    const NAME: &'static str;
}

pub trait TopicBus: Bus {
    type Exchange;
    const TOPIC: &'static str;
}

#[derive(Clone)]
pub struct TopicChannel<E> {
    inner: lapin::Channel,
    _marker: PhantomData<E>,
}

impl<E: Exchange> TopicChannel<E> {
    pub async fn new(connection: &Connection) -> Result<Self> {
        let chan = connection.inner.create_channel().await?;
        chan.exchange_declare(
            E::NAME,
            lapin::ExchangeKind::Topic,
            Default::default(),
            Default::default(),
        )
        .await?;

        Ok(Self {
            inner: chan,
            _marker: PhantomData,
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
impl<E: Exchange> Channel for TopicChannel<E> {
    async fn publish_with_properties(
        &self,
        bytes: &[u8],
        routing_key: &str,
        properties: lapin::BasicProperties,
        correlation_uuid: Uuid,
    ) -> Result<()> {
        let properties = properties.with_correlation_id(correlation_uuid.to_string().into());

        self.inner
            .basic_publish(E::NAME, routing_key, Default::default(), bytes, properties)
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

impl<B: TopicBus> TryFrom<String> for RoutingKey<B> {
    type Error = RoutingKeyError;

    fn try_from(key: String) -> std::result::Result<Self, Self::Error> {
        if key.contains("**") || key.contains("##") {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC));
        }
        let regex = key
            .replace('.', r#"\."#)
            .replace('*', r#"(.*)"#)
            .replace(r"\.#", r#"\.(.*)"#)
            .replace(r"#\.", r#"(.*)\."#);

        let regex = Regex::new(&format!("^{regex}$")).unwrap();
        if !regex.is_match(B::TOPIC) {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC));
        }

        Ok(Self {
            key,
            _marker: PhantomData,
        })
    }
}

impl<B> Display for RoutingKey<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.key.fmt(f)
    }
}

#[derive(Debug)]
pub enum RoutingKeyError {
    InvalidKey(String, &'static str),
    InvalidPart(String, usize, &'static str),
    TooLong(usize, &'static str),
}

impl Display for RoutingKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingKeyError::InvalidKey(key, topic) => {
                write!(f, "Routing key {key} is not valid for topic {topic}")
            }
            RoutingKeyError::InvalidPart(part, pos, topic) => {
                write!(
                    f,
                    "Invalid routing key part '{part}' for topic '{topic}' on position {pos}"
                )
            }
            RoutingKeyError::TooLong(len, topic) => {
                let expected_len = topic.split('c').count();
                write!(f, "Invalid routing key length for topic {topic}. Expected {expected_len}, got {len}")
            }
        }
    }
}

impl std::error::Error for RoutingKeyError {}

#[cfg(test)]
mod tests {
    use crate::{topic_bus, topic_exchange, RoutingKey};

    topic_exchange!(MyExchange, "the_exchange");
    topic_bus!(MyTopic, (), MyExchange, "this.is.a.topic");

    #[test]
    fn test_routing_key() {
        // Valid cases
        RoutingKey::<MyTopic>::try_from("this.*".to_owned()).unwrap();
        RoutingKey::<MyTopic>::try_from("this.*.a.*".to_owned()).unwrap();
        RoutingKey::<MyTopic>::try_from("this.*.a.#".to_owned()).unwrap();
        RoutingKey::<MyTopic>::try_from("this.is.a.topic".to_owned()).unwrap();
        RoutingKey::<MyTopic>::try_from("*.is.a.topic".to_owned()).unwrap();
        RoutingKey::<MyTopic>::try_from("*.*.a.topic".to_owned()).unwrap();
        RoutingKey::<MyTopic>::try_from("#.a.topic".to_owned()).unwrap();

        // Invalid cases
        RoutingKey::<MyTopic>::try_from("this".to_owned()).unwrap_err(); // Too short
        RoutingKey::<MyTopic>::try_from("this.is".to_owned()).unwrap_err(); // Too short
        RoutingKey::<MyTopic>::try_from("that".to_owned()).unwrap_err(); // Invalid word
        RoutingKey::<MyTopic>::try_from("this.is.a.topic.that.is.too.long".to_owned()).unwrap_err(); // Too long
        RoutingKey::<MyTopic>::try_from("this.**".to_owned()).unwrap_err(); // Double *
        RoutingKey::<MyTopic>::try_from("this.**.is".to_owned()).unwrap_err(); // Double *
        RoutingKey::<MyTopic>::try_from("this.##.is".to_owned()).unwrap_err(); // Double #
        RoutingKey::<MyTopic>::try_from("##".to_owned()).unwrap_err(); // Double #
        RoutingKey::<MyTopic>::try_from("this.is.a.topic.*".to_owned()).unwrap_err(); // Too long
        RoutingKey::<MyTopic>::try_from("this.is.a.topic.#".to_owned()).unwrap_err();
        // Too long
    }
}

#[macro_export]
macro_rules! topic_exchange {
    ($exchange:ident, $name:literal) => {
        #[derive(Debug, Clone)]
        pub enum $exchange {}

        impl $crate::Exchange for $exchange {
            const NAME: &'static str = $name;
        }
    };
}

#[macro_export]
macro_rules! topic_bus {
    ($bus:ident, $publish_payload:ty, $exchange:ty, $topic:literal) => {
        $crate::bus!($bus, $publish_payload);

        impl $crate::TopicBus for $bus {
            type Exchange = $exchange;
            const TOPIC: &'static str = $topic;
        }
    };
}
