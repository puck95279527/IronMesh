use openraft::raft::AppendEntriesRequest;
use openraft::raft::VoteRequest;

use crate::raft::model::iron_raft_full_snapshot_request::IronRaftFullSnapshotRequest;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// IronMesh Raft TCP 请求传输模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronRaftTcpRpcRequest {
    AppendEntries(AppendEntriesRequest<IronRaftTypeConfig>), // 追加日志请求。
    Vote(VoteRequest<u64>), // 投票请求。
    FullSnapshot(IronRaftFullSnapshotRequest), // 完整快照请求。
}
