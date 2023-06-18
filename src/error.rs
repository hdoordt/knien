use std::{fmt::Display, error::Error as StdError};

#[cfg(feature = "topic")]
use crate::RoutingKeyError;

#[derive(Debug)]
/// This type represents anything that can go wrong when interacting with [knien](crate).
pub enum Error {
    /// A low-level error concerning RabbitMq
    Mq(lapin::Error),
    /// A (de)serialization error
    Serde(Box<dyn StdError + Send + Sync>),
    /// Error validating a [uuid::Uuid]
    Uuid(uuid::Error),
    #[cfg(feature = "rpc")]
    /// Error involving replying to a [crate::Delivery]
    Reply(crate::ReplyError),
    #[cfg(feature = "topic")]
    /// Error validating a [crate::ConsumerRoutingKey] or a [crate::PublisherRoutingKey]
    RoutingKey(RoutingKeyError),
}

impl From<lapin::Error> for Error {
    fn from(e: lapin::Error) -> Self {
        Self::Mq(e)
    }
}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Self {
        Self::Uuid(e)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Mq(e) => write!(f, "Rabbit MQ error: {e}"),
            Error::Serde(e) => write!(f, "(De)serialization error {e}"),
            Error::Uuid(e) => write!(f, "UUID error: {e}"),
            #[cfg(feature = "rpc")]
            Error::Reply(e) => write!(f, "Reply error: {e}"),
            #[cfg(feature = "topic")]
            Error::RoutingKey(e) => write!(f, "Error creating routing key: {e}"),
        }
    }
}

impl std::error::Error for Error {}
