// 启动注册 Raft 节点。
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::BTreeMap;

    use iron_core_cluster_v2::raft::cluster::iron_raft_boot_node::IronRaftBootNode;
    use iron_core_cluster_v2::raft::cluster::iron_raft_cluster_manager::IronRaftClusterManager;
    use iron_core_cluster_v2::raft::cluster::iron_raft_node::IronRaftNode;

    tracing_subscriber::fmt::init();
    let boot_nodes = BTreeMap::from([
        (1, IronRaftBootNode::new(1, "127.0.0.1:5001")),
        (2, IronRaftBootNode::new(2, "127.0.0.1:5002")),
        (3, IronRaftBootNode::new(3, "127.0.0.1:5003")),
    ]);
    let cluster_manager = IronRaftClusterManager::new(
        IronRaftNode::new(1, "cluster-reg-1", "127.0.0.1:5001"),
        boot_nodes,
    );

    cluster_manager.run().await
}
