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
    AppendEntries(AppendEntriesRequest<IronTypeConfig>), // 追加日志请求。
    Vote(VoteRequest<u64>),                              // 投票请求。
    #[serde(skip)]
    FullSnapshot {
        vote: Vote<u64>,                    // 快照传输携带的投票状态。
        snapshot: Snapshot<IronTypeConfig>, // 完整快照数据。
    },
    JoinCluster {
        node_id: u64,      // 请求加入集群的节点 ID。
        node_addr: String, // 请求加入集群的节点 TCP 地址。
    },
}

// IronMesh Raft TCP 响应消息。
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum IronTcpResponse {
    AppendEntries(Result<AppendEntriesResponse<u64>, RaftError<u64>>), // 追加日志响应。
    Vote(Result<VoteResponse<u64>, RaftError<u64>>),                   // 投票响应。
    FullSnapshot(Result<SnapshotResponse<u64>, Fatal<u64>>),           // 完整快照响应。
    JoinCluster(Result<(), String>),                                   // 节点加入集群响应。
}
