use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::StreamExt;
use lapin::{options::BasicConsumeOptions, BasicProperties};
use tokio::{
    sync::mpsc,
    task::{self, JoinHandle},
};
use uuid::Uuid;

use crate::{delivery_uuid, Bus, Connection, Result};

use super::{Channel, Consumer, Publisher};

#[derive(Clone)]
pub struct RpcChannel {
    inner: lapin::Channel,
    pending_replies: Arc<DashMap<Uuid, mpsc::UnboundedSender<lapin::message::Delivery>>>,
    exchange: String,
}

impl RpcChannel {
    pub async fn new(connection: &Connection, exchange: String) -> Result<RpcChannel> {
        let chan = connection.inner.create_channel().await?;
        if !exchange.is_empty() {
            chan.exchange_declare(
                exchange.as_ref(),
                lapin::ExchangeKind::Topic,
                Default::default(),
                Default::default(),
            )
            .await?;
        }
        let pending_replies: DashMap<Uuid, mpsc::UnboundedSender<lapin::message::Delivery>> =
            DashMap::new();
        let pending_replies = Arc::new(pending_replies);

        let reply_consumer = chan
            .basic_consume(
                "amq.rabbitmq.reply-to",
                &Uuid::new_v4().to_string(),
                BasicConsumeOptions {
                    // Consuming the direct reply-to queue works only in no-ack mode.
                    // See https://www.rabbitmq.com/direct-reply-to.html#usage
                    no_ack: true,
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;

        let handle_replies: JoinHandle<Result<()>> = task::spawn({
            let mut reply_consumer = reply_consumer;
            let pending_replies = pending_replies.clone();
            async move {
                while let Some(msg_res) = reply_consumer.next().await {
                    match msg_res {
                        Ok(msg) => {
                            // Spawn a task that attempts to forward the reply `msg` that just came in
                            // Getting a lock to pending_replies may block
                            let forward_reply: JoinHandle<()> = task::spawn_blocking({
                                let pending_replies = pending_replies.clone();
                                move || {
                                    let Some(msg_id) = delivery_uuid(&msg) else {
                                            todo!("report abcense of uuid");
                                        };
                                    let msg_id = match msg_id {
                                        Ok(i) => i,
                                        Err(_) => todo!("report error"),
                                    };

                                    if let Some(tx) = pending_replies.get(&msg_id) {
                                        tx.send(msg).unwrap();
                                    } else {
                                        todo!("Report absence of sender");
                                    };
                                }
                            });
                            // `forward_reply` should run to completion
                            drop(forward_reply);
                        }
                        Err(e) => eprintln!("Error receiving reply message: {e:?}"),
                    }
                }
                panic!("Task handle_replies ended");
            }
        });
        // `handle_replies` should run forever
        drop(handle_replies);

        Ok(RpcChannel {
            inner: chan,
            pending_replies,
            exchange,
        })
    }

    pub(crate) async fn publish_with_properties(
        &self,
        bytes: &[u8],
        routing_key: &str,
        properties: BasicProperties,
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

    pub async fn consumer<B: Bus>(&self, consumer_tag: &str) -> Result<Consumer<Self, B>> {
        self.inner
            .queue_declare(B::QUEUE, Default::default(), Default::default())
            .await?;
        let consumer = self
            .inner
            .basic_consume(
                B::QUEUE,
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

    pub fn publisher<B>(&self) -> Publisher<Self, B> {
        Publisher {
            chan: self.clone(),
            _marker: PhantomData,
        }
    }

    pub(crate) fn register_pending_reply(
        &self,
        correlation_uuid: Uuid,
        tx: mpsc::UnboundedSender<lapin::message::Delivery>,
    ) {
        self.pending_replies.insert(correlation_uuid, tx);
    }

    pub(crate) fn remove_pending_reply(&self, correlation_uuid: &Uuid) {
        self.pending_replies.remove(correlation_uuid);
    }
}

#[async_trait]
impl Channel for RpcChannel {
    async fn publish_with_properties(
        &self,
        bytes: &[u8],
        routing_key: &str,
        properties: BasicProperties,
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
