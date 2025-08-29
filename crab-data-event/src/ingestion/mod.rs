use std::sync::Arc;

pub mod barter_ingestor;
pub mod types;

/// The common DataEvent Trait, if data sources implement it can own event ability.
pub trait DataEvent {
    #[allow(dead_code)]
    fn id(&self) -> String;
    #[allow(dead_code)]
    fn timestamp(&self) -> i64;
}

// Define the data acquisition behavior trait
pub trait Ingestor<E: DataEvent> {
    fn start(self: Arc<Self>);
}
