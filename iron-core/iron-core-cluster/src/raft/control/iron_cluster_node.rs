// 集群节点角色。
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IronClusterNodeRole {
    // 投票节点，用于参与 Raft 投票。
    Voter,
    // 学习节点，用于作为 learner 加入集群。
    Learner,
}

// IronMesh 集群节点。
#[derive(Debug, Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IronClusterNode {
    // 集群节点 ID。
    pub node_id: u64,
    // 集群节点对外公布 IP，用于写入 membership 并让其他节点连接。
    pub advertise_node_ip: String,
    // 集群节点通信端口，为空时由操作系统分配本地可用端口。
    pub node_port: Option<u16>,
    // 集群节点调试 HTTP 地址。
    pub http_debug_addr: Option<String>,
    // 集群节点是否为唯一首次起盘节点。
    pub is_boot_node: bool,
    // 集群节点角色。
    pub node_role: IronClusterNodeRole,
}

impl IronClusterNode {
    // 创建集群节点配置。
    pub(crate) fn new(
        node_id: u64,
        advertise_node_ip: impl Into<String>,
        node_port: Option<u16>,
        http_debug_addr: Option<String>,
        node_role: IronClusterNodeRole,
    ) -> Self {
        Self {
            node_id,
            advertise_node_ip: advertise_node_ip.into(),
            node_port,
            http_debug_addr,
            is_boot_node: matches!(node_role, IronClusterNodeRole::Voter),
            node_role,
        }
    }

    // 生成当前节点用于 TCP bind 的地址。
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.advertise_node_ip, self.node_port.unwrap_or(0))
    }

    // 生成当前节点对外广播的 TCP 地址。
    pub fn node_addr(&self) -> String {
        format!("{}:{}", self.advertise_node_ip, self.node_port.unwrap_or(0))
    }

    // 写入 TCP listener 实际绑定成功后的端口。
    pub(crate) fn set_resolved_node_port(&mut self, node_port: u16) {
        self.node_port = Some(node_port);
    }

    // 判断当前节点是否为唯一首次起盘节点。
    pub fn is_boot_node(&self) -> bool {
        self.is_boot_node
    }
}
