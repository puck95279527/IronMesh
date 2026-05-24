// IronMesh Raft 节点。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IronRaftNode {
    pub node_id: u64,      // Raft 节点 ID。
    pub node_name: String, // Raft 节点名称。
    pub node_addr: String, // Raft 节点通信地址。
    pub http_debug_addr: Option<String>, // Raft 节点调试 HTTP 地址。
}

impl IronRaftNode {
    // 创建 Raft 节点。
    pub fn new(
        node_id: u64,
        node_name: impl Into<String>,
        node_addr: impl Into<String>,
        http_debug_addr: Option<String>,
    ) -> Self {
        Self {
            node_id,
            node_name: node_name.into(),
            node_addr: node_addr.into(),
            http_debug_addr,
        }
    }
}
