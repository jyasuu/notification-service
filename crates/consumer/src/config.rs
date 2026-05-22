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
    /// Maximum allowed **total** size of all attachments in an event, in bytes.
    ///
    /// This is a cumulative limit across all attachments, not a per-file cap.
    /// For example, with the default of 10 MiB: a single 9 MiB attachment
    /// passes, but two 6 MiB attachments fail.  Events that exceed this limit
    /// are permanently rejected (no retry) to avoid retaining large payloads
    /// in memory across retry cycles.
    ///
    /// If you need a per-file limit, enforce it upstream before publishing the
    /// event.
    pub max_attachment_bytes: usize,
    /// Maximum consecutive rate-limit backoff cycles before a recipient is
    /// permanently marked FAILED.
    ///
    /// Each cycle waits up to `30 * 2^attempt` seconds (capped at 30 * 8 = 240s).
    /// With the default of 5 cycles that is roughly 2–4 minutes of total
    /// rate-limit hold time before giving up.  Increase for providers with
    /// very long cooldown windows; decrease to fail fast during outages.
    pub max_rl_waits: u32,
    /// Hard cap on recipients per AMQP message.
    ///
    /// A single event with thousands of recipients would hold a semaphore permit
    /// and many DB connections for an arbitrarily long time. Events that exceed
    /// this limit are immediately NACK'd to the DLQ so an operator can inspect
    /// them rather than letting them monopolise the worker.
    ///
    /// Default: 500. Raise for bulk-mailing use cases; lower for latency-sensitive
    /// transactional mail where a runaway event should fail fast.
    pub max_recipients_per_event: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            amqp_url: "amqp://guest:guest@localhost:5672".into(),
            queue: "email.requested".into(),
            exchange: "anvil-notify".into(),
            routing_key: "email.requested".into(),
            max_retries: 3,
            retry_base_ms: 1_000,
            max_concurrency: 10,
            max_attachment_bytes: 10 * 1024 * 1024,
            max_rl_waits: 5,
            max_recipients_per_event: 500,
        }
    }
}
