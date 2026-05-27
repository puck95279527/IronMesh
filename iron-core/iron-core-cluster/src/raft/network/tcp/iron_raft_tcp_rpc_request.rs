use openraft::raft::AppendEntriesRequest;
use openraft::raft::VoteRequest;

use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::model::snapshot::iron_raft_full_snapshot_request::IronRaftFullSnapshotRequest;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh Raft TCP 请求传输模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "S::WriteRequest: serde::Serialize",
    deserialize = "S::WriteRequest: serde::de::DeserializeOwned"
))]
pub enum IronRaftTcpRpcRequest<S>
where
    S: IronRaftStateMachineData,
{
    AppendEntries(AppendEntriesRequest<IronRaftTypeConfig<S>>), // 追加日志请求。
    Vote(VoteRequest<u64>),                                     // 投票请求。
    FullSnapshot(IronRaftFullSnapshotRequest),                  // 完整快照请求。
    ClientWrite(S::WriteRequest),                               // 客户端业务写入请求。
    JoinNode {
        node_id: u64,      // 请求加入的节点 ID。
        node_addr: String, // 请求加入的节点地址。
    },
}
