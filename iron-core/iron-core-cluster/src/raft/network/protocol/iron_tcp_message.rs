use openraft::Snapshot;
use openraft::Vote;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

use crate::raft::IronTypeConfig;

// IronMesh Raft TCP 请求消息。
#[derive(Clone, Debug)]
pub enum IronTcpRequest {
    AppendEntries(AppendEntriesRequest<IronTypeConfig>),
    Vote(VoteRequest<u64>),
    FullSnapshot {
        vote: Vote<u64>,
        snapshot: Snapshot<IronTypeConfig>,
    },
}

// IronMesh Raft TCP 响应消息。
#[derive(Debug)]
pub enum IronTcpResponse {
    AppendEntries(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
    FullSnapshot(SnapshotResponse<u64>),
}
