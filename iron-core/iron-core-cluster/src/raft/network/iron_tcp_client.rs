use std::future::Future;

use openraft::Snapshot;
use openraft::Vote;
use openraft::error::Fatal;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

use crate::raft::IronTypeConfig;

// IronMesh Raft TCP 客户端。
#[derive(Clone, Debug, Default)]
pub struct IronTcpClient;

impl RaftNetwork<IronTypeConfig> for IronTcpClient {
    // 发送追加日志请求。
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<IronTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        unimplemented!()
    }

    // 发送投票请求。
    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        unimplemented!()
    }

    // 发送完整快照。
    async fn full_snapshot(
        &mut self,
        _vote: Vote<u64>,
        _snapshot: Snapshot<IronTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<IronTypeConfig, Fatal<u64>>> {
        unimplemented!()
    }
}
