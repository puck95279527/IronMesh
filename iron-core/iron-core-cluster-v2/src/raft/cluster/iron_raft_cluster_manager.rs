use std::collections::BTreeMap;

use crate::raft::cluster::iron_raft_boot_node::IronRaftBootNode;
use crate::raft::cluster::iron_raft_node::IronRaftNode;

// IronMesh Raft 集群管理器。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IronRaftClusterManager {
    pub current_node: IronRaftNode, // 当前 Raft 节点。
    pub boot_nodes: BTreeMap<u64, IronRaftBootNode>, // Raft 启动节点表。
}

impl IronRaftClusterManager {
    // 创建 Raft 集群管理器。
    pub fn new(
        current_node: IronRaftNode,
        boot_nodes: BTreeMap<u64, IronRaftBootNode>,
    ) -> Self {
        Self {
            current_node,
            boot_nodes,
        }
    }
}
