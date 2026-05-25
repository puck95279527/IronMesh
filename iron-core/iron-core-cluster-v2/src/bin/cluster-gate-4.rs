// 启动网关 Raft 节点。
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster_v2::logging::init_cluster_logging;
    use iron_core_cluster_v2::raft::cluster::iron_raft_node::{IronRaftNode, IronRaftNodeRole};
    use iron_core_cluster_v2::raft::cluster::manager::iron_raft_cluster_manager::IronRaftClusterManager;

    init_cluster_logging();
    let cluster_manager = IronRaftClusterManager::new(IronRaftNode::new(
        4,
        "cluster-gate",
        "127.0.0.1:5004",
        Some("127.0.0.1:7104".to_string()),
        IronRaftNodeRole::Normal,
    ))?;

    cluster_manager.start().await?.wait_forever().await
}
