use std::sync::Arc;
use std::time::Duration;

use openraft::OptionalSend;
use openraft::RaftNetwork;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::Vote;
use openraft::error::Fatal;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use tokio::sync::Mutex;

use crate::raft::model::iron_raft_full_snapshot_meta::IronRaftFullSnapshotMeta;
use crate::raft::model::iron_raft_full_snapshot_request::IronRaftFullSnapshotRequest;
use crate::raft::model::iron_raft_full_snapshot_response::IronRaftFullSnapshotResponse;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::iron_raft_tcp_frame::IronRaftTcpFrame;
use crate::raft::network::iron_raft_tcp_rpc_request::IronRaftTcpRpcRequest;
use crate::raft::network::iron_raft_tcp_rpc_response::IronRaftTcpRpcResponse;

// IronMesh Raft TCP 客户端。
#[derive(Debug, Clone)]
pub struct IronRaftTcpClient {
    pub target_node_id: u64, // 目标节点标识。
    pub target_addr: String, // 目标节点 TCP 地址。
    pub cached_stream: Arc<Mutex<Option<tokio::net::TcpStream>>>, // 目标节点长连接缓存。
}

// OpenRaft 标准协议相关方法。
impl IronRaftTcpClient {
    // 创建网络错误。
    fn network_error(
        error: &(impl std::error::Error + 'static),
    ) -> RPCError<u64, openraft::BasicNode, RaftError<u64>> {
        RPCError::Network(NetworkError::new(error))
    }

    // 创建快照网络错误。
    fn snapshot_network_error(
        error: &(impl std::error::Error + 'static),
    ) -> RPCError<u64, openraft::BasicNode, RaftError<u64, InstallSnapshotError>> {
        RPCError::Network(NetworkError::new(error))
    }

    // 构建完整快照元信息传输模型。
    fn build_snapshot_meta(
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
    ) -> IronRaftFullSnapshotMeta {
        let (last_log_term, last_log_node_id, last_log_index) =
            if let Some(log_id) = &meta.last_log_id {
                (
                    Some(log_id.leader_id.term),
                    Some(log_id.leader_id.node_id),
                    Some(log_id.index),
                )
            } else {
                (None, None, None)
            };

        let membership = meta
            .last_membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>();

        IronRaftFullSnapshotMeta {
            snapshot_id: meta.snapshot_id.clone(),
            last_log_term,
            last_log_node_id,
            last_log_index,
            membership,
        }
    }

    // 从完整快照响应中恢复投票状态。
    fn build_vote_from_response(response: IronRaftFullSnapshotResponse) -> Vote<u64> {
        if response.vote_committed {
            Vote::new_committed(response.vote_term, response.vote_node_id)
        } else {
            Vote::new(response.vote_term, response.vote_node_id)
        }
    }

    // 建立一个新的 TCP 连接。
    async fn connect_new_stream(&self) -> Result<tokio::net::TcpStream, std::io::Error> {
        tokio::net::TcpStream::connect(&self.target_addr).await
    }

    // 清空缓存连接。
    async fn clear_cached_stream(&self) {
        let mut guard = self.cached_stream.lock().await;
        *guard = None;
    }

    // 执行一次请求发送与响应读取。
    async fn send_request_once(
        &self,
        request: &IronRaftTcpRpcRequest,
    ) -> Result<IronRaftTcpRpcResponse, std::io::Error> {
        let mut guard = self.cached_stream.lock().await;
        if guard.is_none() {
            *guard = Some(self.connect_new_stream().await?);
        }

        let stream = guard.as_mut().expect("cached stream must exist");
        if let Err(error) = IronRaftTcpFrame::write_json(stream, request).await {
            *guard = None;
            return Err(error);
        }

        match IronRaftTcpFrame::read_json::<IronRaftTcpRpcResponse>(stream).await {
            Ok(response) => Ok(response),
            Err(error) => {
                *guard = None;
                Err(error)
            }
        }
    }

    // 发送请求并在失败后重连重试一次。
    async fn send_request_with_retry(
        &self,
        request: IronRaftTcpRpcRequest,
    ) -> Result<IronRaftTcpRpcResponse, std::io::Error> {
        match self.send_request_once(&request).await {
            Ok(response) => Ok(response),
            Err(_) => self.send_request_once(&request).await,
        }
    }

    // 按 soft_ttl 执行请求，超时时主动清理连接。
    async fn send_request_with_option(
        &self,
        request: IronRaftTcpRpcRequest,
        option: &openraft::network::RPCOption,
    ) -> Result<IronRaftTcpRpcResponse, std::io::Error> {
        match tokio::time::timeout(option.soft_ttl(), self.send_request_with_retry(request)).await {
            Ok(result) => result,
            Err(_) => {
                self.clear_cached_stream().await;
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "raft tcp rpc soft ttl timeout",
                ))
            }
        }
    }
}

// IronMesh 自定义扩展协议相关方法。
impl IronRaftTcpClient {
    // 请求目标节点把当前节点加入集群。
    pub async fn join_node(
        &self,
        node_id: u64,
        node_name: String,
        node_addr: String,
    ) -> Result<(), std::io::Error> {
        let request = IronRaftTcpRpcRequest::JoinNode {
            node_id,
            node_name,
            node_addr,
        };

        match tokio::time::timeout(
            Duration::from_secs(2),
            self.send_request_with_retry(request),
        )
        .await
        {
            Ok(result) => match result? {
                IronRaftTcpRpcResponse::JoinNode(Ok(())) => Ok(()),
                IronRaftTcpRpcResponse::JoinNode(Err(error)) => {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, error))
                }
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected tcp response kind",
                )),
            },
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "raft tcp join node timeout",
            )),
        }
    }
}

impl RaftNetwork<IronRaftTypeConfig> for IronRaftTcpClient {
    // 发送追加日志请求。
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<IronRaftTypeConfig>,
        option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>>
    {
        let request = IronRaftTcpRpcRequest::AppendEntries(rpc);
        let response = self
            .send_request_with_option(request, &option)
            .await
            .map_err(|error| Self::network_error(&error))?;

        match response {
            IronRaftTcpRpcResponse::AppendEntries(result) => result.map_err(|error| {
                RPCError::RemoteError(RemoteError::new(self.target_node_id, error))
            }),
            _ => {
                let error = std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected tcp response kind",
                );
                Err(Self::network_error(&error))
            }
        }
    }

    // 发送安装快照请求。
    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<IronRaftTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, openraft::BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let error = std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "tcp install_snapshot is not used",
        );
        Err(Self::snapshot_network_error(&error))
    }

    // 发送投票请求。
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        let request = IronRaftTcpRpcRequest::Vote(rpc);
        let response = self
            .send_request_with_option(request, &option)
            .await
            .map_err(|error| Self::network_error(&error))?;

        match response {
            IronRaftTcpRpcResponse::Vote(result) => result.map_err(|error| {
                RPCError::RemoteError(RemoteError::new(self.target_node_id, error))
            }),
            _ => {
                let error = std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected tcp response kind",
                );
                Err(Self::network_error(&error))
            }
        }
    }

    // 发送完整快照请求。
    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<IronRaftTypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: openraft::network::RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<IronRaftTypeConfig, Fatal<u64>>> {
        let snapshot_meta = Self::build_snapshot_meta(&snapshot.meta);
        let snapshot_bytes = (*snapshot.snapshot).into_inner();
        let full_snapshot_request = IronRaftFullSnapshotRequest {
            vote_term: vote.leader_id.term,
            vote_node_id: vote.leader_id.node_id,
            vote_committed: vote.committed,
            meta: snapshot_meta,
            snapshot: snapshot_bytes,
        };
        let request = IronRaftTcpRpcRequest::FullSnapshot(full_snapshot_request);
        let response = self
            .send_request_with_option(request, &option)
            .await
            .map_err(|error| StreamingError::Network(NetworkError::new(&error)))?;

        match response {
            IronRaftTcpRpcResponse::FullSnapshot(result) => match result {
                Ok(data) => Ok(SnapshotResponse::new(Self::build_vote_from_response(data))),
                Err(error) => Err(StreamingError::RemoteError(RemoteError::new(
                    self.target_node_id,
                    error,
                ))),
            },
            _ => {
                let error = std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected tcp response kind",
                );
                Err(StreamingError::Network(NetworkError::new(&error)))
            }
        }
    }
}
