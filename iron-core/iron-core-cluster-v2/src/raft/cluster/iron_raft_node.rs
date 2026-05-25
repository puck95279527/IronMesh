// Raft 节点角色。
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IronRaftNodeRole {
    // 启动节点，用于集群初始化。
    Boot,
    // 普通节点，用于集群运行。
    Normal,
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
            node_role,
        }
    }

    // 创建启动节点配置。
    pub fn new_boot(
        node_id: u64,
        node_name: impl Into<String>,
        node_addr: impl Into<String>,
        http_debug_addr: Option<String>,
    ) -> Self {
        Self::new(
            node_id,
            node_name,
            node_addr,
            http_debug_addr,
            IronRaftNodeRole::Boot,
        )
    }

    // 创建普通节点配置。
    pub fn new_normal(
        node_id: u64,
        node_name: impl Into<String>,
        node_addr: impl Into<String>,
        http_debug_addr: Option<String>,
    ) -> Self {
        Self::new(
            node_id,
            node_name,
            node_addr,
            http_debug_addr,
            IronRaftNodeRole::Normal,
        )
    }

    // 判断当前节点是否为启动节点。
    pub fn is_boot_node(&self) -> bool {
        matches!(self.node_role, IronRaftNodeRole::Boot)
    }
}
