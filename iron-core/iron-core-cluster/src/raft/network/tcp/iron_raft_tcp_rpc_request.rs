use openraft::raft::AppendEntriesRequest;
use openraft::raft::VoteRequest;

use crate::raft::model::command::iron_raft_request::IronRaftRequest;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::model::snapshot::iron_raft_full_snapshot_request::IronRaftFullSnapshotRequest;

// IronMesh Raft TCP 请求传输模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronRaftTcpRpcRequest {
    AppendEntries(AppendEntriesRequest<IronRaftTypeConfig>), // 追加日志请求。
    Vote(VoteRequest<u64>),                                  // 投票请求。
    FullSnapshot(IronRaftFullSnapshotRequest),               // 完整快照请求。
    ClientWrite(IronRaftRequest),                            // 客户端业务写入请求。
    JoinNode {
        node_id: u64,      // 请求加入的节点 ID。
        node_name: String, // 请求加入的节点名称。
        node_addr: String, // 请求加入的节点地址。
    },
}
