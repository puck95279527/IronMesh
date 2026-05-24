use std::time::Duration;

use openraft::error::InstallSnapshotError;
use openraft::error::Fatal;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::OptionalSend;
use openraft::RaftNetwork;
use openraft::Snapshot;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// IronMesh Raft HTTP 网络客户端。
#[derive(Debug, Clone)]
pub struct IronRaftNetwork {
    pub target_node_id: u64, // 目标节点标识。
    pub target_addr: String, // 目标节点 HTTP 地址。
}

impl IronRaftNetwork {
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
}

impl RaftNetwork<IronRaftTypeConfig> for IronRaftNetwork {
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
        _vote: openraft::Vote<u64>,
        _snapshot: Snapshot<IronRaftTypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: openraft::network::RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<IronRaftTypeConfig, Fatal<u64>>> {
        let error = std::io::Error::new(std::io::ErrorKind::Unsupported, "full snapshot is not implemented");
        Err(StreamingError::Network(NetworkError::new(&error)))
    }
}
