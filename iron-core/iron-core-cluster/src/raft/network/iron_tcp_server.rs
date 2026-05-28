use std::collections::BTreeSet;
use std::io;

use futures_util::SinkExt;
use futures_util::StreamExt;
use openraft::ChangeMembers;
use openraft::Raft;
use openraft::ServerState;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::raft::IronTypeConfig;
use crate::raft::network::protocol::IronTcpFrameCodec;
use crate::raft::network::protocol::IronTcpRequest;
use crate::raft::network::protocol::IronTcpResponse;

// IronMesh Raft TCP 服务端。
#[derive(Clone)]
pub struct IronTcpServer {
    pub raft: Raft<IronTypeConfig>,   // Raft 节点句柄。
    pub boot_node_ids: BTreeSet<u64>, // 注册节点 ID 表。
}

impl IronTcpServer {
    // 创建 TCP 服务端。
    pub fn new(raft: Raft<IronTypeConfig>, boot_node_ids: BTreeSet<u64>) -> Self {
        Self {
            raft,
            boot_node_ids,
        }
    }

    // 启动 TCP 服务端并持续处理连接。
    pub async fn serve(self, listener: TcpListener) -> Result<(), io::Error> {
        loop {
            let (stream, _) = listener.accept().await?;
            let raft = self.raft.clone();
            let boot_node_ids = self.boot_node_ids.clone();

            tokio::spawn(async move {
                let _ = Self::handle_connection(raft, boot_node_ids, stream).await;
            });
        }
    }

    // 在单个连接上循环处理多个请求。
    async fn handle_connection(
        raft: Raft<IronTypeConfig>,
        boot_node_ids: BTreeSet<u64>,
        stream: TcpStream,
    ) -> Result<(), io::Error> {
        let mut framed = Framed::new(stream, IronTcpFrameCodec::default());

        while let Some(frame) = framed.next().await {
            let request = IronTcpFrameCodec::decode_request(frame?)?;
            let response =
                Self::handle_request(raft.clone(), boot_node_ids.clone(), request).await?;
            let response = IronTcpFrameCodec::encode_response(&response)?;
            framed.send(response).await?;
        }

        Ok(())
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
            IronTcpRequest::FullSnapshot { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "full snapshot tcp request is not implemented",
            )),
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
