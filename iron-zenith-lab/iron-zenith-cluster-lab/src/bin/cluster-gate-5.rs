// 启动网关 Raft 验证节点。
mod support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster::raft::cluster::iron_raft_node::{IronRaftNode, IronRaftNodeRole};
    use iron_core_cluster::raft::cluster::manager::iron_raft_cluster_manager::IronRaftClusterManager;
    use support::cluster_data_writer::write_current_node_cluster_data;
    use support::cluster_logging::init_cluster_process_logging;
    use tokio::time::Duration;
    use tokio::time::MissedTickBehavior;

    init_cluster_process_logging()?;
    let cluster_manager = IronRaftClusterManager::new(IronRaftNode::new(
        5,
        "cluster-gate",
        "127.0.0.1:5005",
        Some("127.0.0.1:7105".to_string()),
        IronRaftNodeRole::Normal,
    ))?;

    let cluster_handle = cluster_manager.start().await?;
    write_current_node_cluster_data(
        &cluster_handle,
        5,
        "cluster-gate",
        "127.0.0.1:5005",
        "normal",
    )
    .await;

    let mut cluster_data_log_interval = tokio::time::interval(Duration::from_secs(1));
    cluster_data_log_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    cluster_data_log_interval.tick().await;

    loop {
        tokio::select! {
            result = cluster_handle.wait_forever() => return result,
            _ = cluster_data_log_interval.tick() => {
                let state_machine_data = cluster_handle.local_state_machine_data().await;
                tracing::debug!(
                    cluster_data_size = state_machine_data.cluster_data.values.len(),
                    "[Iron] [cluster-lab] 本地集群业务数据大小"
                );
            }
        }
    }
}
