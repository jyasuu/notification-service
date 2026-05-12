/// Configuration for the AMQP consumer and retry strategy.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub amqp_url: String,
    /// Queue to consume from.
    pub queue: String,
    /// Exchange for DLQ binding.
    pub exchange: String,
    /// Routing key on the main exchange.
    pub routing_key: String,
    /// Maximum delivery attempts before a message is NACK'd to DLX.
    pub max_retries: u32,
    /// Base delay in milliseconds for exponential backoff.
    pub retry_base_ms: u64,
    /// Maximum number of messages processed concurrently.
    /// Prevents unbounded task spawning under a message burst.
    pub max_concurrency: usize,
    /// Maximum allowed size per fetched attachment in bytes.
    /// Attachments exceeding this are permanently rejected (no retry).
    pub max_attachment_bytes: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            amqp_url: "amqp://guest:guest@localhost:5672".into(),
            queue: "email.requested".into(),
            exchange: "notifications".into(),
            routing_key: "email.requested".into(),
            max_retries: 3,
            retry_base_ms: 1_000,
            max_concurrency: 10,
            max_attachment_bytes: 10 * 1024 * 1024,
        }
    }
}
