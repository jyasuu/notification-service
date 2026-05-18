/// Configuration for the outbox poller.
#[derive(Debug, Clone)]
pub struct OutboxConfig {
    /// Connection URL for the **business** database (may differ from the
    /// anvil-notify's own DB).
    pub database_url: String,

    /// AMQP broker URL.
    pub amqp_url: String,

    /// Exchange to publish events to.
    pub exchange: String,

    /// Routing key used when publishing.
    pub routing_key: String,

    /// Base interval in milliseconds between polling cycles.
    ///
    /// This is the wait time used when the outbox has rows to process.
    /// When the outbox is idle the worker backs off up to 8× this value
    /// to reduce load on the business DB during quiet periods.
    /// Default: 1000 ms.
    pub poll_interval_ms: u64,

    /// Maximum rows to process per polling cycle.
    ///
    /// Smaller values reduce the window during which rows are held IN_PROGRESS,
    /// lowering the blast radius of a worker crash. Tune based on your event
    /// volume: 10–20 is a good default for most deployments.
    /// Default: 10.
    pub batch_size: i64,

    /// Maximum number of connections in the outbox DB pool.
    ///
    /// The outbox worker is single-threaded (one poll loop, one reaper) so it
    /// rarely needs more than 2 connections simultaneously. Keep this low to
    /// avoid eating into the business DB's connection budget, especially when
    /// running multiple anvil-notify replicas.
    /// Default: 2.
    pub pool_size: u32,

    /// How long a row may remain IN_PROGRESS before the reaper considers it
    /// stuck and resets it back to PENDING (seconds).
    ///
    /// A row enters IN_PROGRESS when the worker claims it. If the worker
    /// crashes before marking it PUBLISHED, the row stays IN_PROGRESS forever
    /// without this recovery mechanism. Set to at least 2× your expected
    /// publish latency; the default of 300 s (5 min) is conservative.
    ///
    /// Requires migration 0016_outbox_locked_at.sql to be applied first.
    /// Default: 300 s.
    pub stale_lock_timeout_secs: u64,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://postgres:postgres@localhost:5432/business".into(),
            amqp_url: "amqp://guest:guest@localhost:5672".into(),
            exchange: "anvil-notify".into(),
            routing_key: "email.requested".into(),
            poll_interval_ms: 1_000,
            batch_size: 10,   // reduced from 50 — smaller lock window per cycle
            pool_size: 2,     // reduced from 5 — worker rarely uses more than 2
            stale_lock_timeout_secs: 300, // 5 minutes
        }
    }
}
