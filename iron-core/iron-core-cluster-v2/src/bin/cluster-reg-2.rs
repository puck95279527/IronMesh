// 启动第二个注册 Raft 节点。
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
    let _cluster_manager = IronRaftClusterManager::new(
        IronRaftNode::new(2, "cluster-reg", "127.0.0.1:5002"),
        boot_nodes,
    );

    iron_core_cluster_v2::raft::start_iron_raft_node(2, "127.0.0.1:5002".to_string(), 6002).await
}
