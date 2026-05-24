use std::time::Duration;

use openraft::OptionalSend;
use openraft::RaftNetwork;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::error::Fatal;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RemoteError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::raft::SnapshotResponse;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::Vote;

use crate::raft::model::iron_raft_full_snapshot_request::IronRaftFullSnapshotRequest;
use crate::raft::model::iron_raft_full_snapshot_response::IronRaftFullSnapshotResponse;
use crate::raft::model::iron_raft_full_snapshot_meta::IronRaftFullSnapshotMeta;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// IronMesh Raft HTTP 客户端。
#[derive(Debug, Clone)]
pub struct IronRaftHttpClient {
    pub target_node_id: u64, // 目标节点标识。
    pub target_addr: String, // 目标节点 HTTP 地址。
}

impl IronRaftHttpClient {
    // 生成目标节点的 HTTP 地址。
    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.target_addr, path)
    }

    // 生成最小网络错误。
    fn network_error(error: &(impl std::error::Error + 'static)) -> RPCError<u64, openraft::BasicNode, RaftError<u64>> {
        RPCError::Network(NetworkError::new(error))
    }

    // 生成最小快照网络错误。
    fn snapshot_network_error(
        error: &(impl std::error::Error + 'static),
    ) -> RPCError<u64, openraft::BasicNode, RaftError<u64, InstallSnapshotError>> {
        RPCError::Network(NetworkError::new(error))
    }

    // 创建带超时的 HTTP 客户端。
    fn client(option: &openraft::network::RPCOption) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(option.hard_ttl().max(Duration::from_millis(100)))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    }

    // 构建完整快照元信息传输模型。
    fn build_snapshot_meta(meta: &SnapshotMeta<u64, openraft::BasicNode>) -> IronRaftFullSnapshotMeta {
        let (last_log_term, last_log_node_id, last_log_index) = if let Some(log_id) = &meta.last_log_id {
            (
                Some(log_id.leader_id.term),
                Some(log_id.leader_id.node_id),
                Some(log_id.index),
            )
        } else {
            (None, None, None)
        };

        let membership = meta.last_membership.membership().voter_ids().collect::<Vec<_>>();

        IronRaftFullSnapshotMeta {
            snapshot_id: meta.snapshot_id.clone(),
            last_log_term,
            last_log_node_id,
            last_log_index,
            membership,
        }
    }

    // 还原完整快照响应中的投票状态。
    fn build_vote_from_response(response: IronRaftFullSnapshotResponse) -> Vote<u64> {
        if response.vote_committed {
            Vote::new_committed(response.vote_term, response.vote_node_id)
        } else {
            Vote::new(response.vote_term, response.vote_node_id)
        }
    }

}

impl RaftNetwork<IronRaftTypeConfig> for IronRaftHttpClient {
    // 发送追加日志请求。
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<IronRaftTypeConfig>,
        option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        let response = Self::client(&option)
            .post(self.url("/raft/append"))
            .json(&rpc)
            .send()
            .await
            .map_err(|error| Self::network_error(&error))?
            .error_for_status()
            .map_err(|error| Self::network_error(&error))?;

        response
            .json::<Result<AppendEntriesResponse<u64>, RaftError<u64>>>()
            .await
            .map_err(|error| Self::network_error(&error))?
            .map_err(|error| RPCError::RemoteError(openraft::error::RemoteError::new(self.target_node_id, error)))
    }

    // 发送安装快照请求。
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<IronRaftTypeConfig>,
        option: openraft::network::RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64, InstallSnapshotError>>> {
        let response = Self::client(&option)
            .post(self.url("/raft/snapshot"))
            .json(&rpc)
            .send()
            .await
            .map_err(|error| Self::snapshot_network_error(&error))?
            .error_for_status()
            .map_err(|error| Self::snapshot_network_error(&error))?;

        response
            .json::<Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>>()
            .await
            .map_err(|error| Self::snapshot_network_error(&error))?
            .map_err(|error| RPCError::RemoteError(openraft::error::RemoteError::new(self.target_node_id, error)))
    }

    // 发送投票请求。
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        let response = Self::client(&option)
            .post(self.url("/raft/vote"))
            .json(&rpc)
            .send()
            .await
            .map_err(|error| Self::network_error(&error))?
            .error_for_status()
            .map_err(|error| Self::network_error(&error))?;

        response
            .json::<Result<VoteResponse<u64>, RaftError<u64>>>()
            .await
            .map_err(|error| Self::network_error(&error))?
            .map_err(|error| RPCError::RemoteError(openraft::error::RemoteError::new(self.target_node_id, error)))
    }

    // 发送完整快照。
    async fn full_snapshot(
        &mut self,
        vote: openraft::Vote<u64>,
        snapshot: Snapshot<IronRaftTypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: openraft::network::RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<IronRaftTypeConfig, Fatal<u64>>> {
        let snapshot_meta = Self::build_snapshot_meta(&snapshot.meta);
        let snapshot_bytes = (*snapshot.snapshot).into_inner();

        let request = IronRaftFullSnapshotRequest {
            vote_term: vote.leader_id.term,
            vote_node_id: vote.leader_id.node_id,
            vote_committed: vote.committed,
            meta: snapshot_meta,
            snapshot: snapshot_bytes,
        };

        let response = Self::client(&option)
            .post(self.url("/raft/full-snapshot"))
            .json(&request)
            .send()
            .await
            .map_err(|error| StreamingError::Network(NetworkError::new(&error)))?
            .error_for_status()
            .map_err(|error| StreamingError::Network(NetworkError::new(&error)))?;

        let result = response
            .json::<Result<IronRaftFullSnapshotResponse, Fatal<u64>>>()
            .await
            .map_err(|error| StreamingError::Network(NetworkError::new(&error)))?;

        match result {
            Ok(data) => Ok(SnapshotResponse::new(Self::build_vote_from_response(data))),
            Err(error) => Err(StreamingError::RemoteError(RemoteError::new(self.target_node_id, error))),
        }
    }
}
