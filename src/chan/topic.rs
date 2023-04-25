use std::marker::PhantomData;
use std::{any::type_name, fmt::Display};

use async_trait::async_trait;
use lapin::options::QueueDeclareOptions;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

use crate::{fmt_correlation_id, Bus, Channel, Connection, Consumer, Publisher, Result};

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
    /// May contain `*` to indicate single-word wildcards, but not `#`
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
        debug!("Created topic channel for exchange {}", E::NAME);
        Ok(Self {
            inner: chan,
            _marker: PhantomData,
        })
    }

    /// Create a new [Consumer] for the topic bus that consumes
    /// messages with routing keys matching the passed [ConsumerRoutingKey].
    pub async fn consumer<B: TopicBus>(
        &self,
        routing_key: ConsumerRoutingKey<B>,
        consumer_tag: &str,
    ) -> Result<Consumer<B>> {
        let queue = self
            .inner
            .queue_declare(
                "",
                QueueDeclareOptions {
                    // Clean up queue on channel disconnection
                    exclusive: true,
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;
        let queue_name = queue.name().as_str();
        self.inner
            .queue_bind(
                queue_name,
                E::NAME,
                &routing_key.key,
                Default::default(),
                Default::default(),
            )
            .await?;
        let consumer = self
            .inner
            .basic_consume(
                queue_name,
                consumer_tag,
                Default::default(),
                Default::default(),
            )
            .await?;
        debug!(
            "Created consumer for topic bus {} with routing key {routing_key} and consumer tag {consumer_tag}",
            type_name::<B>()
        );
        Ok(Consumer {
            inner: consumer,
            _marker: PhantomData,
        })
    }

    /// Create a new [Publisher] that publishes onto the [TopicBus].
    pub fn publisher<B: TopicBus<Chan = Self>>(&self) -> Publisher<B> {
        debug!("Created publisher for topic bus {}", type_name::<B>());
        Publisher { chan: self.clone() }
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
        reply_uuid: Option<Uuid>,
    ) -> Result<()> {
        let correlation_id = fmt_correlation_id(correlation_uuid, reply_uuid);
        let properties = properties.with_correlation_id(correlation_id.into());

        debug!("Publishing message with correlation UUID {correlation_uuid} on Topic Exchange '{}' with routing key {routing_key}", E::NAME);
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

impl<'p, B> Publisher<B>
where
    B: TopicBus,
    B::PublishPayload: Deserialize<'p> + Serialize,
{
    /// Publish a message onto a topic on the exchange associated with the [TopicBus] for this [Publisher] with the passed [PublisherRoutingKey].
    pub async fn publish_topic(
        &self,
        routing_key: PublisherRoutingKey<B>,
        payload: &B::PublishPayload,
    ) -> Result<()> {
        let correlation_uuid = Uuid::new_v4();
        self.publish_with_properties(
            &routing_key.key,
            payload,
            Default::default(),
            correlation_uuid,
            None,
        )
        .await
    }
}

/// A Routing key that can be used to consume messages from a [TopicBus].
/// [ConsumerRoutingKey]s cannot contain `#` and must be at least as
/// concrete as [TopicBus::TOPIC_PATTERN].
#[derive(Debug)]
pub struct ConsumerRoutingKey<B> {
    key: String,
    _marker: PhantomData<B>,
}

impl<B: TopicBus> TryFrom<String> for ConsumerRoutingKey<B> {
    type Error = RoutingKeyError;

    fn try_from(key: String) -> std::result::Result<Self, Self::Error> {
        // Don't accept pounds
        if key.contains('#') {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC_PATTERN));
        }
        if key.starts_with('.') || key.ends_with('.') || key.contains("..") {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC_PATTERN));
        }
        if key.matches('.').count() != B::TOPIC_PATTERN.matches('.').count() {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC_PATTERN));
        }

        let pattern = B::TOPIC_PATTERN
            .replace('.', r"\.")
            .replace('*', r"([[:alnum:]]*|\*)");

        let regex = Regex::new(&format!("^{pattern}$")).unwrap();
        if !regex.is_match(&key) {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC_PATTERN));
        }
        Ok(Self {
            key,
            _marker: PhantomData,
        })
    }
}

impl<B: TopicBus> TryFrom<&str> for ConsumerRoutingKey<B> {
    type Error = <Self as TryFrom<String>>::Error;

    fn try_from(key: &str) -> std::result::Result<Self, Self::Error> {
        <Self as TryFrom<String>>::try_from(key.to_owned())
    }
}

impl<B> Display for ConsumerRoutingKey<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.key.fmt(f)
    }
}

/// A Routing key that can be used to publish messages on a [TopicBus].
/// Can only represent concrete routing keys, i.e. routing keys cannot contain wildcards.
#[derive(Debug)]
pub struct PublisherRoutingKey<B> {
    key: String,
    _marker: PhantomData<B>,
}

impl<B: TopicBus> TryFrom<String> for PublisherRoutingKey<B> {
    type Error = RoutingKeyError;

    fn try_from(key: String) -> std::result::Result<Self, Self::Error> {
        // Don't accept wildcards
        if key.contains('*') || key.contains('#') {
            return Err(RoutingKeyError::AbstractPublishKey(key));
        }
        if key.starts_with('.') || key.ends_with('.') || key.contains("..") {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC_PATTERN));
        }
        if key.matches('.').count() != B::TOPIC_PATTERN.matches('.').count() {
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC_PATTERN));
        }

        let pattern = B::TOPIC_PATTERN
            .replace('.', r"\.")
            .replace('*', r"[[:alnum:]]*");

        let regex = Regex::new(&format!("^{pattern}$")).unwrap();
        if !regex.is_match(&key) {
            println!("Err");
            return Err(RoutingKeyError::InvalidKey(key, B::TOPIC_PATTERN));
        }
        Ok(Self {
            key,
            _marker: PhantomData,
        })
    }
}

impl<B: TopicBus> TryFrom<&str> for PublisherRoutingKey<B> {
    type Error = <Self as TryFrom<String>>::Error;

    fn try_from(key: &str) -> std::result::Result<Self, Self::Error> {
        <Self as TryFrom<String>>::try_from(key.to_owned())
    }
}

impl<B> Display for PublisherRoutingKey<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.key.fmt(f)
    }
}

#[derive(Debug)]
/// Error indicating what went wrong in setting up a [ConsumerRoutingKey] or a [PublisherRoutingKey]
pub enum RoutingKeyError {
    /// Got a routing key that does not match the topic pattern associated with the [TopicBus].
    InvalidKey(String, &'static str),
    /// Got a publish routing key that contained wildcards.
    AbstractPublishKey(String),
}

impl Display for RoutingKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingKeyError::InvalidKey(key, topic) => {
                write!(f, "Routing key {key} is not valid for topic {topic}")
            }
            RoutingKeyError::AbstractPublishKey(key) => write!(
                f,
                "Routing key meant for publishing cannot contain wildcards: {key}"
            ),
        }
    }
}

impl std::error::Error for RoutingKeyError {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        topic_bus, topic_exchange, Connection, Consumer, ConsumerRoutingKey, FramePayload,
        Publisher, PublisherRoutingKey, TopicChannel, RABBIT_MQ_URL,
    };
    use futures::StreamExt;
    use test_case::test_case;
    use tokio::{sync::oneshot, time::timeout};
    use uuid::Uuid;

    topic_exchange!(MyExchange, "the_exchange");

    topic_bus!(MyTopic, FramePayload, MyExchange, "frame.*.*");

    #[tokio::test]
    async fn publish() -> crate::Result<()> {
        let connection = Connection::connect(RABBIT_MQ_URL).await.unwrap();
        let uuid = Uuid::new_v4();
        let (tx, rx) = oneshot::channel();
        tokio::task::spawn({
            let channel: TopicChannel<MyExchange> = TopicChannel::new(&connection).await.unwrap();
            let mut consumer: Consumer<MyTopic> = channel
                .consumer("frame.*.*".try_into().unwrap(), &Uuid::new_v4().to_string())
                .await?;
            async move {
                let msg = consumer.next().await.unwrap().unwrap();
                msg.ack(false).await.unwrap();
                let payload = msg.get_payload().unwrap();
                assert_eq!(payload.message, uuid.to_string());
                tx.send(()).unwrap();
            }
        });

        let channel: TopicChannel<MyExchange> = TopicChannel::new(&connection).await.unwrap();
        let publisher: Publisher<MyTopic> = channel.publisher();

        publisher
            .publish_topic(
                "frame.1.2".try_into().unwrap(),
                &FramePayload {
                    message: uuid.to_string(),
                },
            )
            .await
            .unwrap();

        timeout(Duration::from_secs(3), rx).await.unwrap().unwrap();

        Ok(())
    }

    #[test_case("frame.123.456"; "1")]
    fn test_valid_publish_routing_key(key: &str) {
        PublisherRoutingKey::<MyTopic>::try_from(key.to_owned()).unwrap();
    }

    #[test_case(""; "Empty 1")]
    #[test_case("frame..#"; "Double '.' 2")]
    #[test_case("frame.**.#"; "Double '*' 3")]
    #[test_case("frame.##"; "Double '#' 4")]
    #[test_case("frame"; "Too short 5")]
    #[test_case("frame.123"; "Too short 6")]
    #[test_case("frame.123.*"; "Abstract 7")]
    #[test_case("frame.*.456"; "Abstract 8")]
    #[test_case("frame.*.*"; "Abstract 9")]
    #[test_case("#"; "Abstract 10")]
    #[test_case("test.123.456"; "Invalid prefix 11")]
    #[test_case("frame.123.456.789"; "Too long 12")]
    #[test_case("frame.@.&"; "Invalid characters 13")]
    fn test_invalid_publish_routing_key(key: &str) {
        PublisherRoutingKey::<MyTopic>::try_from(key.to_owned()).unwrap_err();
    }

    #[test_case("frame.*.*"; "1")]
    #[test_case("frame.123.*"; "2")]
    #[test_case("frame.*.456"; "3")]
    #[test_case("frame.123.456"; "4")]
    fn test_valid_consume_routing_key(key: &str) {
        ConsumerRoutingKey::<MyTopic>::try_from(key.to_owned()).unwrap();
    }

    #[test_case(""; "Empty 1")]
    #[test_case("frame..#"; "Double '.' 2")]
    #[test_case("frame.**.#"; "Double '*' 3")]
    #[test_case("frame.##"; "Double '#'4")]
    #[test_case("*"; "More abstract than pattern 5")]
    // RabbitMQ accepts this, but it would result in consumption of all messages on the topic exchange
    #[test_case("#"; "Hash 6")]
    #[test_case("*.*.*"; "More abstract than pattern 7")]
    #[test_case("frame.*.*.*"; "Too long 8")]
    #[test_case("test.*.*"; "Invalid word 9")]
    #[test_case("frame.123.*.*"; "Too long 10")]
    #[test_case("frame.*.456.*"; "Too long 11")]
    #[test_case("frame.*.*.789"; "Too long 12")]
    #[test_case("frame.123.*.789"; "Too long 13")]
    #[test_case("frame.123.456.*"; "Too long 14")]
    #[test_case("frame.*.456.789"; "Too long 15")]
    #[test_case("frame.124.456.789"; "Too long 16")]
    #[test_case("frame.#.456.789"; "Too long 17")]
    #[test_case("#.456.789"; "Not starting with 'frame' 18")]
    #[test_case("#.frame.456.789"; "Too long 19")]
    #[test_case("frame.#"; "Contains pound sign 20")]
    #[test_case("frame.#.*"; "Contains pound sign 21")]
    #[test_case("frame.*.#"; "Contains pound sign 22")]
    #[test_case("frame.#.456"; "Contains pound sign 23")]
    #[test_case("frame.123.#"; "Contains pound sign 24")]
    fn test_invalid_consume_routing_key(key: &str) {
        ConsumerRoutingKey::<MyTopic>::try_from(key.to_owned()).unwrap_err();
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
        $crate::bus!($doc, $bus);

        $crate::bus_impl!($bus, $crate::TopicChannel<$exchange>, $publish_payload);

        $crate::topic_bus_impl!($bus, $exchange, $topic);
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

#[doc(hidden)]
#[macro_export]
macro_rules! topic_bus_impl {
    ($bus:ident, $exchange:ty, $topic:literal) => {
        impl $crate::TopicBus for $bus {
            type Exchange = $exchange;
            const TOPIC_PATTERN: &'static str = $topic;
        }
    };
}
