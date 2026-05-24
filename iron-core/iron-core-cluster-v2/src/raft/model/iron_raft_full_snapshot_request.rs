use crate::raft::model::iron_raft_full_snapshot_meta::IronRaftFullSnapshotMeta;

// IronMesh 完整快照请求传输模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IronRaftFullSnapshotRequest {
    pub vote_term: u64, // 请求携带的投票任期。
    pub vote_node_id: u64, // 请求携带的投票节点。
    pub vote_committed: bool, // 请求携带的投票提交状态。
    pub meta: IronRaftFullSnapshotMeta, // 快照元信息。
    pub snapshot: Vec<u8>, // 快照完整字节数据。
}
