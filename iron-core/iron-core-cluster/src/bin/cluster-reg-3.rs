// 启动注册 Raft 节点。
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster::logging::init_cluster_logging;
    use iron_core_cluster::raft::cluster::iron_raft_node::{IronRaftNode, IronRaftNodeRole};
    use iron_core_cluster::raft::cluster::manager::iron_raft_cluster_manager::IronRaftClusterManager;

    init_cluster_logging();
    let cluster_manager = IronRaftClusterManager::new(IronRaftNode::new(
        3,
        "cluster-reg-3",
        "127.0.0.1:5003",
        Some("127.0.0.1:7103".to_string()),
        IronRaftNodeRole::Boot,
    ))?;

    cluster_manager.start().await?.wait_forever().await
}
