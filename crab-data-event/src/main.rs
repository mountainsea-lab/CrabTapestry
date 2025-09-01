use crab_data_event::server;

#[tokio::main]
async fn main() {
    server::start().await;
}
