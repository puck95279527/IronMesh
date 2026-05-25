// 启动注册 Raft 节点。
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::BTreeMap;

    use iron_core_cluster_v2::logging::init_cluster_logging;
    use iron_core_cluster_v2::raft::cluster::iron_raft_cluster_manager::IronRaftClusterManager;
    use iron_core_cluster_v2::raft::cluster::iron_raft_node::IronRaftNode;

    init_cluster_logging();
    let boot_nodes = BTreeMap::from([
        (
            1,
            IronRaftNode::new_boot(1, "cluster-reg-1", "127.0.0.1:5001", Some("127.0.0.1:7101".to_string())),
        ),
        (
            2,
            IronRaftNode::new_boot(2, "cluster-reg-2", "127.0.0.1:5002", Some("127.0.0.1:7102".to_string())),
        ),
        (
            3,
            IronRaftNode::new_boot(3, "cluster-reg-3", "127.0.0.1:5003", Some("127.0.0.1:7103".to_string())),
        ),
    ]);
    let cluster_manager = IronRaftClusterManager::new(
        IronRaftNode::new_boot(
            1,
            "cluster-reg-1",
            "127.0.0.1:5001",
            Some("127.0.0.1:7101".to_string()),
        ),
        boot_nodes,
    );

    cluster_manager.run().await
}
