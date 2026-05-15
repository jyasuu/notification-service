pub mod config;
pub mod processor;
pub mod runner;

#[cfg(test)]
mod tests;

pub use config::ConsumerConfig;
pub use runner::run_consumer;
