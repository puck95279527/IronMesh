// IronMesh 完整快照元信息传输模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IronRaftFullSnapshotMeta {
    pub snapshot_id: String, // 快照唯一标识。
    pub last_log_term: Option<u64>, // 快照最后日志任期。
    pub last_log_node_id: Option<u64>, // 快照最后日志节点标识。
    pub last_log_index: Option<u64>, // 快照最后日志索引。
    pub membership: Vec<u64>, // 快照成员节点列表。
}
