use std::collections::BTreeSet;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::StreamExt;
use openraft::ChangeMembers;
use openraft::Raft;
use openraft::ServerState;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::TryAcquireError;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use crate::control_plane::iron_cluster_config::RAFT_TCP_MAX_CONNECTIONS;
use crate::control_plane::iron_cluster_config::RAFT_TCP_READ_TIMEOUT;
use crate::control_plane::iron_cluster_config::RAFT_TCP_WRITE_TIMEOUT;
use crate::raft::IronTypeConfig;
use crate::raft::network::iron_network_factory::IronRaftNetworkEvent;
use crate::raft::network::protocol::IronTcpFrameCodec;
use crate::raft::network::protocol::IronTcpRequest;
use crate::raft::network::protocol::IronTcpResponse;

// IronMesh Raft TCP 服务端。
#[derive(Clone)]
pub struct IronTcpServer {
    pub raft: Raft<IronTypeConfig>,                   // Raft 节点句柄。
    pub boot_node_ids: BTreeSet<u64>,                 // 注册节点 ID 表。
    connection_limiter: Arc<Semaphore>,               // TCP 连接并发限制器。
    event_sender: mpsc::Sender<IronRaftNetworkEvent>, // TCP 连接事件发送器。
}

impl IronTcpServer {
    // 创建 TCP 服务端。
    pub(crate) fn new(
        raft: Raft<IronTypeConfig>,
        boot_node_ids: BTreeSet<u64>,
        event_sender: mpsc::Sender<IronRaftNetworkEvent>,
    ) -> Self {
        Self {
            raft,
            boot_node_ids,
            connection_limiter: Arc::new(Semaphore::new(RAFT_TCP_MAX_CONNECTIONS)),
            event_sender,
        }
    }

    // 启动 TCP 服务端并持续处理连接。
    pub async fn serve(self, listener: TcpListener) -> Result<(), io::Error> {
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let permit = match self.connection_limiter.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(TryAcquireError::NoPermits) => {
                    tracing::warn!(
                        %peer_addr,
                        max_connections = RAFT_TCP_MAX_CONNECTIONS,
                        "[Iron] [cluster] Raft TCP 连接数已达上限，拒绝新连接"
                    );
                    continue;
                }
                Err(TryAcquireError::Closed) => {
                    return Err(io::Error::other("Raft TCP 连接限制器已关闭"));
                }
            };

            let raft = self.raft.clone();
            let boot_node_ids = self.boot_node_ids.clone();
            let event_sender = self.event_sender.clone();

            tokio::spawn(async move {
                let result =
                    Self::handle_connection(raft, boot_node_ids, stream, peer_addr, permit).await;
                let error_message = match &result {
                    Ok(()) => "Raft TCP 连接已关闭".to_string(),
                    Err(error) => {
                        Self::log_connection_error(peer_addr, error);
                        error.to_string()
                    }
                };
                Self::report_local_connection_closed(event_sender, peer_addr, error_message).await;
            });
        }
    }

    // 上报本地 TCP 连接关闭事件。
    async fn report_local_connection_closed(
        event_sender: mpsc::Sender<IronRaftNetworkEvent>,
        peer_addr: SocketAddr,
        error_message: String,
    ) {
        let event = IronRaftNetworkEvent::LocalConnectionClosed {
            peer_addr: peer_addr.to_string(),
            error_message,
        };

        if event_sender.send(event).await.is_err() {
            tracing::warn!(
                %peer_addr,
                "[Iron] [cluster] Raft TCP 本地断线事件接收任务已关闭"
            );
        }
    }

    // 在单个连接上循环处理多个请求。
    async fn handle_connection(
        raft: Raft<IronTypeConfig>,
        boot_node_ids: BTreeSet<u64>,
        stream: TcpStream,
        peer_addr: SocketAddr,
        _connection_permit: OwnedSemaphorePermit,
    ) -> Result<(), io::Error> {
        let mut framed = Framed::new(stream, IronTcpFrameCodec::default());

        loop {
            let frame = match tokio::time::timeout(RAFT_TCP_READ_TIMEOUT, framed.next()).await {
                Ok(Some(frame)) => frame?,
                Ok(None) => return Ok(()),
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("Raft TCP 读取请求超时: peer_addr={peer_addr}"),
                    ));
                }
            };

            let request = serde_json::from_slice::<IronTcpRequest>(&frame)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let response =
                Self::handle_request(raft.clone(), boot_node_ids.clone(), request).await?;
            let response = serde_json::to_vec(&response)
                .map(Bytes::from)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            tokio::time::timeout(RAFT_TCP_WRITE_TIMEOUT, framed.send(response))
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("Raft TCP 写入响应超时: peer_addr={peer_addr}"),
                    )
                })??;
        }
    }

    // 记录 TCP 连接处理错误。
    fn log_connection_error(peer_addr: SocketAddr, error: &io::Error) {
        match error.kind() {
            io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe => {
                tracing::debug!(
                    %peer_addr,
                    %error,
                    "[Iron] [cluster] Raft TCP 连接已断开"
                );
            }
            _ => {
                tracing::warn!(
                    %peer_addr,
                    %error,
                    "[Iron] [cluster] Raft TCP 连接处理失败"
                );
            }
        }
    }

    // 处理单个 TCP 请求。
    async fn handle_request(
        raft: Raft<IronTypeConfig>,
        boot_node_ids: BTreeSet<u64>,
        request: IronTcpRequest,
    ) -> Result<IronTcpResponse, io::Error> {
        match request {
            IronTcpRequest::AppendEntries(rpc) => Ok(IronTcpResponse::AppendEntries(
                raft.append_entries(rpc).await,
            )),
            IronTcpRequest::Vote(rpc) => Ok(IronTcpResponse::Vote(raft.vote(rpc).await)),
            IronTcpRequest::InstallSnapshot(rpc) => {
                #[allow(deprecated)]
                let result = raft.install_snapshot(rpc).await;
                Ok(IronTcpResponse::InstallSnapshot(result))
            }
            IronTcpRequest::JoinCluster { node_id, node_addr } => {
                let result =
                    Self::handle_join_cluster(raft, boot_node_ids, node_id, node_addr).await;
                Ok(IronTcpResponse::JoinCluster(result))
            }
        }
    }

    // 处理节点加入集群请求。
    async fn handle_join_cluster(
        raft: Raft<IronTypeConfig>,
        boot_node_ids: BTreeSet<u64>,
        node_id: u64,
        node_addr: String,
    ) -> Result<(), String> {
        let metrics = raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader {
            return Err(format!(
                "当前节点不是 leader，current_leader={:?}, node_id={}",
                metrics.current_leader, node_id
            ));
        }

        if metrics
            .membership_config
            .membership()
            .get_node(&node_id)
            .is_some()
        {
            tracing::info!(
                node_id,
                node_addr = %node_addr,
                "[Iron] [cluster] 节点已经在集群中，跳过重复加入"
            );
            return Ok(());
        }

        tracing::info!(
            node_id,
            node_addr = %node_addr,
            "[Iron] [cluster] leader 收到节点加入集群请求"
        );

        raft.add_learner(node_id, openraft::BasicNode::new(node_addr.clone()), true)
            .await
            .map_err(|error| error.to_string())?;

        if boot_node_ids.contains(&node_id) {
            raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([node_id])), true)
                .await
                .map_err(|error| error.to_string())?;
        }

        tracing::info!(
            node_id,
            node_addr = %node_addr,
            "[Iron] [cluster] leader 已将节点加入集群"
        );
        Ok(())
    }
}
