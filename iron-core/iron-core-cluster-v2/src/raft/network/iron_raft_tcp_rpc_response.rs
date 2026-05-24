use openraft::error::Fatal;
use openraft::error::RaftError;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteResponse;

use crate::raft::model::iron_raft_full_snapshot_response::IronRaftFullSnapshotResponse;

// IronMesh Raft TCP 响应传输模型。
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum IronRaftTcpRpcResponse {
    AppendEntries(Result<AppendEntriesResponse<u64>, RaftError<u64>>), // 追加日志响应。
    Vote(Result<VoteResponse<u64>, RaftError<u64>>), // 投票响应。
    FullSnapshot(Result<IronRaftFullSnapshotResponse, Fatal<u64>>), // 完整快照响应。
}
