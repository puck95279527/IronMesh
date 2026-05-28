use std::future::Future;
use std::io;

use futures_util::SinkExt;
use futures_util::StreamExt;
use openraft::Snapshot;
use openraft::Vote;
use openraft::error::Fatal;
use openraft::error::NetworkError;
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
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use crate::raft::IronTypeConfig;
use crate::raft::network::iron_network_factory::IronRaftNetworkEvent;
use crate::raft::network::protocol::IronTcpFrameCodec;
use crate::raft::network::protocol::IronTcpRequest;
use crate::raft::network::protocol::IronTcpResponse;

// IronMesh Raft TCP 客户端。
#[derive(Clone, Debug, Default)]
pub struct IronTcpClient {
    pub target_node_id: Option<u64>, // 目标节点标识。
    pub target_addr: String,         // 目标节点 TCP 地址。
    pub(crate) event_sender: Option<mpsc::Sender<IronRaftNetworkEvent>>, // 可选的 TCP 连接事件发送器。
}

impl IronTcpClient {
    // 创建 TCP 客户端。
    pub fn new(target_addr: String) -> Self {
        Self {
            target_node_id: None,
            target_addr,
            event_sender: None,
        }
    }

    // 创建 Raft RPC TCP 客户端。
    pub(crate) fn new_raft_client(
        target_node_id: u64,
        target_addr: String,
        event_sender: Option<mpsc::Sender<IronRaftNetworkEvent>>,
    ) -> Self {
        Self {
            target_node_id: Some(target_node_id),
            target_addr,
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

    // 发送追加日志请求。
    async fn send_append_entries(
        &self,
        rpc: AppendEntriesRequest<IronTypeConfig>,
    ) -> Result<AppendEntriesResponse<u64>, io::Error> {
        let result = self
            .send_request(IronTcpRequest::AppendEntries(rpc))
            .await
            .and_then(|response| match response {
                IronTcpResponse::AppendEntries(result) => {
                    result.map_err(|error| io::Error::new(io::ErrorKind::Other, error))
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected append entries response",
                )),
            });

        self.report_raft_rpc_failure(&result).await;
        result
    }

    // 发送投票请求。
    async fn send_vote(&self, rpc: VoteRequest<u64>) -> Result<VoteResponse<u64>, io::Error> {
        let result = self
            .send_request(IronTcpRequest::Vote(rpc))
            .await
            .and_then(|response| match response {
                IronTcpResponse::Vote(result) => {
                    result.map_err(|error| io::Error::new(io::ErrorKind::Other, error))
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected vote response",
                )),
            });

        self.report_raft_rpc_failure(&result).await;
        result
    }

    // 发送加入集群请求。
    pub async fn join_cluster(&self, node_id: u64, node_addr: String) -> Result<(), io::Error> {
        match self
            .send_request(IronTcpRequest::JoinCluster { node_id, node_addr })
            .await?
        {
            IronTcpResponse::JoinCluster(result) => result.map_err(io::Error::other),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected join cluster response",
            )),
        }
    }

    // 创建网络错误。
    fn network_error(
        error: &(impl std::error::Error + 'static),
    ) -> RPCError<u64, openraft::BasicNode, RaftError<u64>> {
        RPCError::Network(NetworkError::new(error))
    }

    // 上报 Raft RPC 连接失败事件。
    async fn report_raft_rpc_failure<T>(&self, result: &Result<T, io::Error>) {
        let Err(error) = result else {
            return;
        };

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
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>>
    {
        self.send_append_entries(rpc)
            .await
            .map_err(|error| Self::network_error(&error))
    }

    // 发送投票请求。
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        self.send_vote(rpc)
            .await
            .map_err(|error| Self::network_error(&error))
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
