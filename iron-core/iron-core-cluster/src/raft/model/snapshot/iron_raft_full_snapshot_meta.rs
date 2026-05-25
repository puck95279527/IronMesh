// IronMesh 完整快照元信息传输模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IronRaftFullSnapshotMeta {
    pub snapshot_id: String,                             // 快照唯一标识。
    pub last_log_term: Option<u64>,                      // 快照最后日志任期。
    pub last_log_node_id: Option<u64>,                   // 快照最后日志节点标识。
    pub last_log_index: Option<u64>,                     // 快照最后日志索引。
    pub membership_configs: Vec<Vec<u64>>,               // 快照成员投票配置。
    pub membership_nodes: Vec<IronRaftFullSnapshotNode>, // 快照成员节点地址。
}

// IronMesh 完整快照成员节点传输模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IronRaftFullSnapshotNode {
    pub node_id: u64,      // 成员节点标识。
    pub node_addr: String, // 成员节点 TCP 地址。
}
