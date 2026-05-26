// 启动注册 Raft 验证节点。
mod support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster::raft::cluster::iron_raft_node::{IronRaftNode, IronRaftNodeRole};
    use iron_core_cluster::raft::cluster::manager::iron_raft_cluster_manager::IronRaftClusterManager;
    use support::cluster_data_writer::write_current_node_cluster_data;
    use support::cluster_logging::init_cluster_process_logging;

    init_cluster_process_logging()?;
    let cluster_manager = IronRaftClusterManager::new(IronRaftNode::new(
        3,
        "cluster-reg-3",
        "127.0.0.1:5003",
        Some("127.0.0.1:7103".to_string()),
        IronRaftNodeRole::Voter,
    ))?;

    let cluster_handle = cluster_manager.start().await?;
    write_current_node_cluster_data(
        &cluster_handle,
        3,
        "cluster-reg-3",
        "127.0.0.1:5003",
        "boot",
    )
    .await;
    cluster_handle.wait_forever().await
}
