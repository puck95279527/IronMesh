use std::future::Future;
use std::io;
use std::sync::Arc;

use futures_util::SinkExt;
use futures_util::StreamExt;
use openraft::Snapshot;
use openraft::Vote;
use openraft::error::Fatal;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::snapshot_transport::Chunked;
use openraft::network::snapshot_transport::SnapshotTransport;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use crate::control_plane::iron_cluster_config::CLUSTER_JOIN_REQUEST_TIMEOUT;
use crate::raft::IronTypeConfig;
use crate::raft::network::iron_network_factory::IronRaftNetworkEvent;
use crate::raft::network::protocol::IronTcpFrameCodec;
use crate::raft::network::protocol::IronTcpRequest;
use crate::raft::network::protocol::IronTcpResponse;

// Raft TCP 目标节点连接缓存。
pub type IronTcpCachedStream = Arc<Mutex<Option<TcpStream>>>;

// IronMesh Raft TCP 客户端。
#[derive(Clone, Debug, Default)]
pub struct IronTcpClient {
    pub target_node_id: Option<u64>,        // 目标节点标识。
    pub target_addr: String,                // 目标节点 TCP 地址。
    pub cached_stream: IronTcpCachedStream, // Raft RPC 目标节点长连接缓存。
    pub(crate) event_sender: Option<mpsc::Sender<IronRaftNetworkEvent>>, // 可选的 TCP 连接事件发送器。
}

impl IronTcpClient {
    // 创建 TCP 客户端。
    pub fn new(target_addr: String) -> Self {
        Self {
            target_node_id: None,
            target_addr,
            cached_stream: Arc::new(Mutex::new(None)),
            event_sender: None,
        }
    }

    // 创建 Raft RPC TCP 客户端。
    pub(crate) fn new_raft_client(
        target_node_id: u64,
        target_addr: String,
        cached_stream: IronTcpCachedStream,
        event_sender: Option<mpsc::Sender<IronRaftNetworkEvent>>,
    ) -> Self {
        Self {
            target_node_id: Some(target_node_id),
            target_addr,
            cached_stream,
            event_sender,
        }
    }

    // 发送一个 TCP 请求。
    async fn send_request(&self, request: IronTcpRequest) -> Result<IronTcpResponse, io::Error> {
        let stream = TcpStream::connect(&self.target_addr).await?;
        let mut framed = Framed::new(stream, IronTcpFrameCodec::default());
        let request = IronTcpFrameCodec::encode_request(&request)?;

        framed.send(request).await?;

        let response = framed
            .next()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "tcp response closed"))??;

        IronTcpFrameCodec::decode_response(response)
    }

    // 清空缓存连接。
    async fn clear_cached_stream(&self) {
        let mut guard = self.cached_stream.lock().await;
        if guard.is_some() {
            tracing::debug!(
                target_node_id = ?self.target_node_id,
                target_addr = %self.target_addr,
                "[Iron] [cluster] Raft TCP 缓存连接已清空"
            );
        }
        *guard = None;
    }

    // 通过缓存连接发送一个 Raft RPC 请求。
    async fn send_raft_request_once(
        &self,
        request: &IronTcpRequest,
    ) -> Result<IronTcpResponse, io::Error> {
        let mut guard = self.cached_stream.lock().await;
        if guard.is_none() {
            tracing::debug!(
                target_node_id = ?self.target_node_id,
                target_addr = %self.target_addr,
                "[Iron] [cluster] Raft TCP 缓存连接不存在，创建新连接"
            );
            *guard = Some(TcpStream::connect(&self.target_addr).await?);
        }

        let stream = guard.as_mut().expect("cached stream must exist");
        let mut framed = Framed::new(stream, IronTcpFrameCodec::default());
        let request = IronTcpFrameCodec::encode_request(request)?;

        if let Err(error) = framed.send(request).await {
            tracing::debug!(
                target_node_id = ?self.target_node_id,
                target_addr = %self.target_addr,
                %error,
                "[Iron] [cluster] Raft TCP 写入失败，清空缓存连接"
            );
            *guard = None;
            return Err(error);
        }

        let response = match framed.next().await {
            Some(Ok(response)) => response,
            Some(Err(error)) => {
                tracing::debug!(
                    target_node_id = ?self.target_node_id,
                    target_addr = %self.target_addr,
                    %error,
                    "[Iron] [cluster] Raft TCP 读取失败，清空缓存连接"
                );
                *guard = None;
                return Err(error);
            }
            None => {
                tracing::debug!(
                    target_node_id = ?self.target_node_id,
                    target_addr = %self.target_addr,
                    "[Iron] [cluster] Raft TCP 连接提前关闭，清空缓存连接"
                );
                *guard = None;
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "tcp response closed",
                ));
            }
        };

        IronTcpFrameCodec::decode_response(response)
    }

    // 按 OpenRaft soft ttl 发送 Raft RPC 请求。
    async fn send_raft_request_with_option(
        &self,
        request: IronTcpRequest,
        option: &RPCOption,
    ) -> Result<IronTcpResponse, io::Error> {
        match tokio::time::timeout(option.soft_ttl(), async {
            match self.send_raft_request_once(&request).await {
                Ok(response) => Ok(response),
                Err(error) => {
                    tracing::debug!(
                        target_node_id = ?self.target_node_id,
                        target_addr = %self.target_addr,
                        %error,
                        "[Iron] [cluster] Raft TCP 请求首次失败，准备重试一次"
                    );
                    match self.send_raft_request_once(&request).await {
                        Ok(response) => Ok(response),
                        Err(error) => {
                            tracing::debug!(
                                target_node_id = ?self.target_node_id,
                                target_addr = %self.target_addr,
                                %error,
                                "[Iron] [cluster] Raft TCP 请求重试失败，上报断线事件"
                            );
                            self.report_raft_rpc_failure(&error).await;
                            Err(error)
                        }
                    }
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.clear_cached_stream().await;
                let error =
                    io::Error::new(io::ErrorKind::TimedOut, "raft tcp rpc soft ttl timeout");
                self.report_raft_rpc_failure(&error).await;
                Err(error)
            }
        }
    }

    // 发送加入集群请求。
    pub async fn join_cluster(&self, node_id: u64, node_addr: String) -> Result<(), io::Error> {
        let response = tokio::time::timeout(
            CLUSTER_JOIN_REQUEST_TIMEOUT,
            self.send_request(IronTcpRequest::JoinCluster { node_id, node_addr }),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "join cluster request timeout"))??;

        match response {
            IronTcpResponse::JoinCluster(result) => result.map_err(io::Error::other),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected join cluster response",
            )),
        }
    }

    // 创建网络错误。
    fn network_error<E>(
        error: &(impl std::error::Error + 'static),
    ) -> RPCError<u64, openraft::BasicNode, E>
    where
        E: std::error::Error + 'static,
    {
        RPCError::Network(NetworkError::new(error))
    }

    // 上报 Raft RPC 连接失败事件。
    async fn report_raft_rpc_failure(&self, error: &io::Error) {
        let (Some(target_node_id), Some(event_sender)) = (self.target_node_id, &self.event_sender)
        else {
            return;
        };

        let event = IronRaftNetworkEvent {
            target_node_id,
            target_addr: self.target_addr.clone(),
            error_message: error.to_string(),
        };

        if event_sender.send(event).await.is_err() {
            tracing::warn!(
                target_node_id,
                target_addr = %self.target_addr,
                "[Iron] [cluster] Raft TCP 断线事件接收任务已关闭"
            );
        }
    }
}

impl RaftNetwork<IronTypeConfig> for IronTcpClient {
    // 发送追加日志请求。
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<IronTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>>
    {
        self.send_raft_request_with_option(IronTcpRequest::AppendEntries(rpc), &option)
            .await
            .and_then(|response| match response {
                IronTcpResponse::AppendEntries(result) => {
                    result.map_err(|error| io::Error::new(io::ErrorKind::Other, error))
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected append entries response",
                )),
            })
            .map_err(|error| Self::network_error(&error))
    }

    // 发送投票请求。
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        self.send_raft_request_with_option(IronTcpRequest::Vote(rpc), &option)
            .await
            .and_then(|response| match response {
                IronTcpResponse::Vote(result) => {
                    result.map_err(|error| io::Error::new(io::ErrorKind::Other, error))
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected vote response",
                )),
            })
            .map_err(|error| Self::network_error(&error))
    }

    // 发送安装快照分片请求。
    #[allow(deprecated)]
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<IronTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, openraft::BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let response = self
            .send_raft_request_with_option(IronTcpRequest::InstallSnapshot(rpc), &option)
            .await
            .map_err(|error| Self::network_error(&error))?;
        let Some(target_node_id) = self.target_node_id else {
            let error = io::Error::new(
                io::ErrorKind::InvalidInput,
                "install snapshot raft client missing target node id",
            );
            return Err(Self::network_error(&error));
        };

        match response {
            IronTcpResponse::InstallSnapshot(result) => result.map_err(|error| {
                RPCError::RemoteError(RemoteError::new_with_node(
                    target_node_id,
                    openraft::BasicNode::new(self.target_addr.clone()),
                    error,
                ))
            }),
            _ => {
                let error = io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected install snapshot response",
                );
                Err(Self::network_error(&error))
            }
        }
    }

    // 发送完整快照。
    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<IronTypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<IronTypeConfig, Fatal<u64>>> {
        Chunked::send_snapshot(self, vote, snapshot, cancel, option).await
    }
}
