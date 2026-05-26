// Raft 节点角色。
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IronRaftNodeRole {
    // 投票节点，用于参与 Raft 投票。
    Voter,
    // 学习节点，用于作为 learner 加入集群。
    Learner,
}

// IronMesh Raft 节点。
#[derive(Debug, Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IronRaftNode {
    // Raft 节点 ID。
    pub node_id: u64,
    // Raft 节点名称。
    pub node_name: String,
    // Raft 节点通信地址。
    pub node_addr: String,
    // Raft 节点调试 HTTP 地址。
    pub http_debug_addr: Option<String>,
    // Raft 节点是否为唯一首次起盘节点。
    pub is_boot_node: bool,
    // Raft 节点角色。
    pub node_role: IronRaftNodeRole,
}

impl IronRaftNode {
    // 创建 Raft 节点配置。
    pub fn new(
        node_id: u64,
        node_name: impl Into<String>,
        node_addr: impl Into<String>,
        http_debug_addr: Option<String>,
        node_role: IronRaftNodeRole,
    ) -> Self {
        Self {
            node_id,
            node_name: node_name.into(),
            node_addr: node_addr.into(),
            http_debug_addr,
            is_boot_node: matches!(node_role, IronRaftNodeRole::Voter),
            node_role,
        }
    }

    // 判断当前节点是否为唯一首次起盘节点。
    pub fn is_boot_node(&self) -> bool {
        self.is_boot_node
    }
}
