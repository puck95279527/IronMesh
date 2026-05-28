use openraft::Snapshot;
use openraft::Vote;
use openraft::error::Fatal;
use openraft::error::RaftError;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

use crate::raft::IronTypeConfig;

// IronMesh Raft TCP 请求消息。
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound = "")]
pub enum IronTcpRequest {
    AppendEntries(AppendEntriesRequest<IronTypeConfig>),
    Vote(VoteRequest<u64>),
    #[serde(skip)]
    FullSnapshot {
        vote: Vote<u64>,
        snapshot: Snapshot<IronTypeConfig>,
    },
}

// IronMesh Raft TCP 响应消息。
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum IronTcpResponse {
    AppendEntries(Result<AppendEntriesResponse<u64>, RaftError<u64>>),
    Vote(Result<VoteResponse<u64>, RaftError<u64>>),
    FullSnapshot(Result<SnapshotResponse<u64>, Fatal<u64>>),
}
