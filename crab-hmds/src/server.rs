mod ingestor_service_flow;
mod response;
mod routes;

use crate::global::init_global_services;
use crate::server::ingestor_service_flow::start_ingestor_service_flow;
use ms_tracing::tracing_utils::internal::info;
use ms_tracing::{LogCache, LogEntry, setup_tracing_with_broadcast};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use warp::Filter;

const APPLICATION_NAME: &str = "crab-hmds";

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

    info!("Starting crab-hmds server...");

    // init global comments domain
    let _ = init_global_services().await;

    // ========== 启动数据维护控制服务（启动 -> 实时数据服务+历史数据服务 -> 落盘） ==========
    tokio::spawn(async move {
        start_ingestor_service_flow().await;
    });

    let bind_address: SocketAddr = "127.0.0.1:10088".parse().unwrap();

    // init app
    let app_state = AppState { tx: tx.clone(), cache: cache.clone() };

    let routes = routes::routes(app_state).with(warp::log(APPLICATION_NAME));

    warp::serve(routes).run(bind_address).await;

    info!("You can access the server at {}", bind_address);
}
