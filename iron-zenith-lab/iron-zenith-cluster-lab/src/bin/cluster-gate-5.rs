// 启动网关 Raft 验证节点。
mod support;

use std::sync::Arc;

// 打印当前节点本地集群业务数据大小。
async fn log_local_cluster_data_size(
    cluster_handle: &iron_core_cluster::cluster_api::iron_cluster_handle::IronClusterHandle,
) {
    let state_machine_data = cluster_handle.local_state_machine_data().await;
    tracing::debug!(
        target: "iron_zenith_cluster_lab",
        cluster_data_size = state_machine_data.cluster_data.values.len(),
        "[Iron] [cluster-lab] 本地集群业务数据大小"
    );
}

// 启动当前节点本地集群业务数据大小日志任务。
fn spawn_local_cluster_data_size_logger(
    cluster_handle: Arc<iron_core_cluster::cluster_api::iron_cluster_handle::IronClusterHandle>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            log_local_cluster_data_size(&cluster_handle).await;
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster::raft::cluster::manager::iron_raft_cluster_manager::IronRaftClusterManager;
    use support::cluster_data_writer::write_current_node_cluster_data;
    use support::cluster_logging::init_cluster_process_logging;

    init_cluster_process_logging()?;
    let cluster_manager = IronRaftClusterManager::add_learner(5, "127.0.0.1:5005")?;

    let cluster_handle = Arc::new(cluster_manager.start().await?);
    spawn_local_cluster_data_size_logger(cluster_handle.clone());
    write_current_node_cluster_data(&cluster_handle, 5, "127.0.0.1:5005", "normal").await;
    cluster_handle.wait_forever().await
}
