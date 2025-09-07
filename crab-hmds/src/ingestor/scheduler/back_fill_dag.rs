pub mod back_fill_scheduler;

// /// 历史数据回填调度器 trait
// #[async_trait]
// pub trait BackfillScheduler: Send + Sync {
//     /// 添加 DAG 节点任务
//     async fn add_task(&self, task: BackfillTask) -> Result<usize>;
//     // 返回节点唯一 ID，便于依赖管理
//
//     /// 启动 DAG 调度
//     async fn run(&self) -> Result<()>;
//
//     /// 停止调度
//     async fn stop(&self);
//
//     /// 可选：查询当前节点状态
//     async fn status(&self, node_id: usize) -> Option<NodeStatus>;
//
//     /// 可选：查询就绪队列节点
//     async fn ready_nodes(&self) -> Vec<usize>;
// }
