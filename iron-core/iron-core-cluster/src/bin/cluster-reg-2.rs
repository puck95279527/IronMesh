// 启动注册 Raft 节点。
mod support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster::raft::cluster::iron_raft_node::{IronRaftNode, IronRaftNodeRole};
    use iron_core_cluster::raft::cluster::manager::iron_raft_cluster_manager::IronRaftClusterManager;
    use support::cluster_logging::init_cluster_process_logging;

    init_cluster_process_logging()?;
    let cluster_manager = IronRaftClusterManager::new(IronRaftNode::new(
        2,
        "cluster-reg-2",
        "127.0.0.1:5002",
        Some("127.0.0.1:7102".to_string()),
        IronRaftNodeRole::Boot,
    ))?;

    cluster_manager.start().await?.wait_forever().await
}
