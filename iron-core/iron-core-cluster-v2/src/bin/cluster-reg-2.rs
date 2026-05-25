// 启动注册 Raft 节点。
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster_v2::logging::init_cluster_logging;
    use iron_core_cluster_v2::raft::cluster::iron_raft_cluster_manager::IronRaftClusterManager;
    use iron_core_cluster_v2::raft::cluster::iron_raft_node::{IronRaftNode, IronRaftNodeRole};

    init_cluster_logging();
    let cluster_manager = IronRaftClusterManager::new(IronRaftNode::new(
        2,
        "cluster-reg-2",
        "127.0.0.1:5002",
        Some("127.0.0.1:7102".to_string()),
        IronRaftNodeRole::Boot,
    ))?;

    cluster_manager.run().await
}
