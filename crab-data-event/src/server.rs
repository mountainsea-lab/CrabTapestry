mod data_event_flow;
#[allow(dead_code)]
mod response;
mod routes;

use crate::global::init_global_services;
use crate::server::data_event_flow::start_data_event_flow;
use ms_tracing::tracing_utils::internal::info;
use ms_tracing::{LogCache, LogEntry, setup_tracing_with_broadcast};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use warp::Filter;

const APPLICATION_NAME: &str = "crab_hmds-data-event";

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

    info!("Starting crab_hmds-data-event server...");

    // init global comments domain
    let _ = init_global_services().await;

    // ========== 启动数据事件流（订阅 -> 聚合 -> 发布） ==========
    tokio::spawn(async move {
        start_data_event_flow().await;
    });

    let bind_address: SocketAddr = "127.0.0.1:10086".parse().unwrap();

    // init app
    let app_state = AppState { tx: tx.clone(), cache: cache.clone() };

    let routes = routes::routes(app_state).with(warp::log(APPLICATION_NAME));

    warp::serve(routes).run(bind_address).await;

    info!("You can access the server at {}", bind_address);
}
