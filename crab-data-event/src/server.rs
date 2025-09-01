mod routes;
mod response;

use std::net::SocketAddr;
use std::sync::Arc;
use ms_tracing::{setup_tracing_with_broadcast, LogCache, LogEntry};
use ms_tracing::tracing_utils::internal::info;
use tokio::sync::broadcast;
use crate::global::init_global_services;
use warp::Filter;

const APPLICATION_NAME: &str = "crab-data-event";


#[derive(Clone)]
pub struct AppState {
    tx: broadcast::Sender<LogEntry>,
    cache: LogCache,
}

pub async fn start() {
    // 创建广播通道用于实时日志
    let (tx, _) = broadcast::channel::<LogEntry>(100);

    // 创建共享缓存用于历史日志查询
    let cache: LogCache = Arc::new(tokio::sync::RwLock::new(Vec::new()));

    // 初始化 tracing 日志系统
    setup_tracing_with_broadcast(tx.clone(), cache.clone());


    info!("Starting crab-data-event server...");

    // init global comments domain
    init_global_services().await;



    let bind_address: SocketAddr = "127.0.0.1:10099".parse().unwrap();

    // init app
    let _app_state = AppState {
        tx: tx.clone(),
        cache: cache.clone(),
    };

    info!("You can access the server at {}", bind_address);
}
