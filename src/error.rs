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

#[derive(Debug)]
pub enum ReplyError {
    NoCorrelationUuid,
    NoReplyToConfigured,
}
