// IronMesh Raft 启动节点。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IronRaftBootNode {
    pub node_id: u64,     // Raft 节点 ID。
    pub node_addr: String, // Raft 节点通信地址。
}

impl IronRaftBootNode {
    // 创建 Raft 启动节点。
    pub fn new(node_id: u64, node_addr: impl Into<String>) -> Self {
        Self {
            node_id,
            node_addr: node_addr.into(),
        }
    }
}
