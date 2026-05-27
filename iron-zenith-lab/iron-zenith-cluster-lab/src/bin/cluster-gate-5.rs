// 启动网关 Raft 验证节点。
mod support;

use std::sync::Arc;

// 打印当前节点本地集群业务数据大小。
async fn log_local_cluster_data_size(cluster_handler: &iron_core_cluster::IronClusterHandler) {
    let state_machine_data = cluster_handler.local_state_machine_data().await;
    tracing::debug!(
        target: "iron_zenith_cluster_lab",
        cluster_data_size = state_machine_data.cluster_data.values.len(),
        "[Iron] [cluster-lab] 本地集群业务数据大小"
    );
}

// 启动当前节点本地集群业务数据大小日志任务。
fn spawn_local_cluster_data_size_logger(
    cluster_handler: Arc<iron_core_cluster::IronClusterHandler>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            log_local_cluster_data_size(&cluster_handler).await;
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster::IronClusterManager;
    use support::cluster_data_writer::write_current_node_cluster_data;
    use support::cluster_logging::init_cluster_process_logging;

    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_learner("127.0.0.1")?;

    let cluster_handler = Arc::new(cluster_manager.start().await?);
    let node_id = cluster_handler.current_node_id();
    let node_addr = cluster_handler.current_node_addr();
    spawn_local_cluster_data_size_logger(cluster_handler.clone());
    write_current_node_cluster_data(&cluster_handler, node_id, &node_addr, "normal").await;
    cluster_handler.wait_forever().await
}
