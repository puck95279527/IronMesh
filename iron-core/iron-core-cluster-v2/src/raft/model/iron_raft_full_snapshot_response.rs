// IronMesh 完整快照响应传输模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IronRaftFullSnapshotResponse {
    pub vote_term: u64, // 响应中的投票任期。
    pub vote_node_id: u64, // 响应中的投票节点。
    pub vote_committed: bool, // 响应中的投票提交状态。
}
