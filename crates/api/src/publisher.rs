use anyhow::Context;
use common::AppError;
use lapin::{
    options::BasicPublishOptions, types::FieldTable, BasicProperties, Channel, Connection,
    ConnectionProperties,
};

/// Thin wrapper around an AMQP channel used by the HTTP API to re-enqueue
/// events whose DB rows have been reset to PENDING via the retry endpoints.
///
/// The channel is created once at startup and shared across handlers via
/// `ApiState`. If the connection drops, the publish returns an error and
/// the caller should surface a 503 — the client can retry the HTTP call.
#[derive(Clone)]
pub struct Publisher {
    channel: Channel,
    exchange: String,
    routing_key: String,
}

impl Publisher {
    pub async fn connect(
        amqp_url: &str,
        exchange: &str,
        routing_key: &str,
    ) -> anyhow::Result<Self> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default())
            .await
            .context("Publisher: failed to connect to RabbitMQ")?;
        let channel = conn
            .create_channel()
            .await
            .context("Publisher: create channel")?;

        // Declare the exchange (idempotent — safe if consumer already declared it).
        channel
            .exchange_declare(
                exchange,
                lapin::ExchangeKind::Direct,
                lapin::options::ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context("Publisher: exchange_declare")?;

        Ok(Self {
            channel,
            exchange: exchange.to_owned(),
            routing_key: routing_key.to_owned(),
        })
    }

    /// Publish a raw JSON body to the configured exchange + routing key.
    /// Uses publisher confirms — waits until the broker acknowledges the message.
    pub async fn publish(&self, body: Vec<u8>) -> Result<(), AppError> {
        self.channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                BasicPublishOptions::default(),
                &body,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2), // persistent
            )
            .await
            .map_err(|e| AppError::Queue(e.to_string()))?
            .await
            .map_err(|e| AppError::Queue(e.to_string()))?;
        Ok(())
    }
}
