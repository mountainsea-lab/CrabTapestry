use std::sync::Arc;

mod types;
mod barter_ingestor;

/// The common DataEvent Trait, if data sources implement it can own event ability.
pub trait DataEvent {
    fn id(&self) -> String;
    fn timestamp(&self) -> u64;
}

// Define the data acquisition behavior trait
pub trait Ingestor<E: DataEvent> {
    fn start(self: Arc<Self>);
}
