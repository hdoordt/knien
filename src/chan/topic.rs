use std::fmt::Display;
use std::marker::PhantomData;

use async_trait::async_trait;
use regex::Regex;
use uuid::Uuid;

use crate::{Bus, Channel, Connection, Consumer, Publisher, Result};

/// A Topic Exchange
pub trait TopicExchange: Clone + Send + Sync {
    /// The name of the Topic Exchange
    const NAME: &'static str;
}

/// A bus that is associated with a [TopicExchange], and defines a
/// pattern of topics onto which messages can be publised and consumed
pub trait TopicBus: Bus {
    /// The Topic Exchange this bus is associated with
    type Exchange: TopicExchange;
    /// The pattern of the topic that this bus publishes to or consumes from
    /// May contain `*`
    const TOPIC_PATTERN: &'static str;
}

#[derive(Clone)]
/// A Topic Channel associated with a [TopicExchange].
pub struct TopicChannel<E> {
    inner: lapin::Channel,
    _marker: PhantomData<E>,
}

impl<E: TopicExchange> TopicChannel<E> {
    /// Create a new [TopicChannel], declaring the Topic Exchange
    /// this channel is associated with.
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

    /// Create a new [Consumer] for the topic bus that consumes
    /// messages with routing keys matching the passed [RoutingKey].
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

    /// Create a new [Publisher] that publishes onto the [TopicBus].
    pub fn publisher<B: TopicBus>(&self) -> Publisher<Self, B> {
        Publisher {
            chan: self.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<E: TopicExchange> Channel for TopicChannel<E> {
    async fn publish_with_properties(
        &self,
        payload_bytes: &[u8],
        routing_key: &str,
        properties: lapin::BasicProperties,
        correlation_uuid: Uuid,
    ) -> Result<()> {
        let properties = properties.with_correlation_id(correlation_uuid.to_string().into());

        self.inner
            .basic_publish(
                E::NAME,
                routing_key,
                Default::default(),
                payload_bytes,
                properties,
            )
            .await?;

        Ok(())
    }
}

/// A Routing key that can be used to consume messages from a [TopicBus].
#[derive(Debug)]
pub struct RoutingKey<B> {
    key: String,
    _marker: PhantomData<B>,
}

impl<B: TopicBus> TryFrom<String> for RoutingKey<B> {
    type Error = RoutingKeyError;

    fn try_from(key: String) -> std::result::Result<Self, Self::Error> {
        if key.contains("**")
            || key.contains("##")
            || key.contains("..")
            || key.starts_with('.')
            || key.ends_with('.')
        {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC_PATTERN));
        }
        let regex = key.replace('.', r#"\."#).replace(['*', '#'], r#"(.*)"#);

        let regex = Regex::new(&format!("^{regex}$")).unwrap();
        if !regex.is_match(B::TOPIC_PATTERN) {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC_PATTERN));
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
/// Error indicating what went wrong in setting up a [RoutingKey]
pub enum RoutingKeyError {
    /// Got a key that does not match the topic pattern associated with the [TopicBus].
    InvalidKey(String, &'static str),
}

impl Display for RoutingKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingKeyError::InvalidKey(key, topic) => {
                write!(f, "Routing key {key} is not valid for topic {topic}")
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
        RoutingKey::<MyTopic>::try_from("#".to_owned()).unwrap();

        // Invalid cases
        RoutingKey::<MyTopic>::try_from("this".to_owned()).unwrap_err(); // Too short
        RoutingKey::<MyTopic>::try_from("this.is".to_owned()).unwrap_err(); // Too short
        RoutingKey::<MyTopic>::try_from("that.*".to_owned()).unwrap_err(); // Invalid word
        RoutingKey::<MyTopic>::try_from("and.this.is.a.topic".to_owned()).unwrap_err(); // Invalid word
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
/// Declare a new [TopicExchange], specifying its type identifier and name.
macro_rules! topic_exchange {
    ($doc:literal, $exchange:ident, $name:literal) => {
        #[derive(Debug, Clone)]
        #[doc = $doc]
        pub enum $exchange {}

        impl $crate::TopicExchange for $exchange {
            const NAME: &'static str = $name;
        }
    };
    (doc = $doc:literal, exchange = $exchange:ident, name = $name:literal) => {
        $crate::topic_exchange!($doc, $exchange, $name);
    };
    ($exchange:ident, $name:literal) => {
        $crate::topic_exchange!("", $exchange, $name);
    };
    (exchange = $exchange:ident, name = $name:literal) => {
        $crate::topic_exchange!($exchange, $name);
    };
}

#[macro_export]
/// Declare a new [TopicBus].
macro_rules! topic_bus {
    ($doc:literal, $bus:ident, $publish_payload:ty, $exchange:ty, $topic:literal) => {
        $crate::bus!($bus, $publish_payload);

        impl $crate::TopicBus for $bus {
            type Exchange = $exchange;
            const TOPIC_PATTERN: &'static str = $topic;
        }
    };
    (doc = $doc:literal, bus = $bus:ident, publish = $publish_payload:ty, exchange = $exchange:ty, topic = $topic:literal) => {
        $crate::topic_bus!($doc, $bus, $publish_payload, $exchange, $topic);
    };
    ($bus:ident, $publish_payload:ty, $exchange:ty, $topic:literal) => {
        $crate::topic_bus!("", $bus, $publish_payload, $exchange, $topic);
    };
    (bus = $bus:ident, publish = $publish_payload:ty, exchange = $exchange:ty, topic = $topic:literal) => {
        $crate::topic_bus!($bus, $publish_payload, $exchange, $topic);
    };
}
