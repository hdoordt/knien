use std::fmt::Display;

#[derive(Debug)]
pub enum Error {
    Mq(lapin::Error),
    Serde(serde_json::Error),
    Uuid(uuid::Error),
    Reply(ReplyError),
}

impl From<lapin::Error> for Error {
    fn from(e: lapin::Error) -> Self {
        Self::Mq(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
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
            Error::Reply(e) => write!(f, "Reply error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
pub enum ReplyError {
    NoCorrelationUuid,
    NoReplyToConfigured,
}

impl Display for ReplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplyError::NoCorrelationUuid => {
                write!(f, "No correlation Uuid configured for the message")
            }
            ReplyError::NoReplyToConfigured => {
                write!(f, "No value configured for the reply-to field")
            }
        }
    }
}
