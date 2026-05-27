use openraft::error::Fatal;
use openraft::error::RaftError;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteResponse;

use crate::raft::model::snapshot::iron_raft_full_snapshot_response::IronRaftFullSnapshotResponse;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh Raft TCP 响应传输模型。
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S::WriteResponse: serde::Serialize",
    deserialize = "S::WriteResponse: serde::de::DeserializeOwned"
))]
pub enum IronRaftTcpRpcResponse<S>
where
    S: IronRaftStateMachineData,
{
    AppendEntries(Result<AppendEntriesResponse<u64>, RaftError<u64>>), // 追加日志响应。
    Vote(Result<VoteResponse<u64>, RaftError<u64>>),                   // 投票响应。
    FullSnapshot(Result<IronRaftFullSnapshotResponse, Fatal<u64>>),    // 完整快照响应。
    ClientWrite(Result<S::WriteResponse, String>),                     // 客户端业务写入响应。
    JoinNode(Result<(), String>),                                      // 节点加入响应。
}
