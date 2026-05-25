use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::ChangeMembers;
use openraft::CommittedLeaderId;
use openraft::Membership;
use openraft::Raft;
use openraft::ServerState;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use openraft::Vote;
use tokio::sync::Semaphore;

use crate::raft::iron_raft_constants::MAX_TCP_CONNECTIONS;
use crate::raft::iron_raft_log_tag::peer_tag as peer_node_tag;
use crate::raft::model::command::iron_raft_request::IronRaftRequest;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::model::snapshot::iron_raft_full_snapshot_meta::IronRaftFullSnapshotMeta;
use crate::raft::model::snapshot::iron_raft_full_snapshot_response::IronRaftFullSnapshotResponse;
use crate::raft::network::tcp::iron_raft_tcp_frame::IronRaftTcpFrame;
use crate::raft::network::tcp::iron_raft_tcp_rpc_request::IronRaftTcpRpcRequest;
use crate::raft::network::tcp::iron_raft_tcp_rpc_response::IronRaftTcpRpcResponse;

// IronMesh Raft TCP 服务端。
#[derive(Clone)]
pub struct IronRaftTcpServer {
    // Raft 节点句柄。
    pub raft: Raft<IronRaftTypeConfig>,
    // 需要参与投票的注册节点 ID。
    pub boot_node_ids: BTreeSet<u64>,
}

// OpenRaft 标准协议相关实现。
impl IronRaftTcpServer {
    // 创建 TCP 服务端。
    pub fn new(raft: Raft<IronRaftTypeConfig>, boot_node_ids: BTreeSet<u64>) -> Self {
        Self {
            raft,
            boot_node_ids,
        }
    }

    // 启动 TCP 服务端并持续处理连接。
    pub async fn serve(self, tcp_addr: String) -> Result<(), Box<dyn std::error::Error>> {
        let addr = tcp_addr.parse::<SocketAddr>()?;
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(%tcp_addr, "[Iron] [cluster] 启动 Raft TCP 服务");

        let boot_node_ids = self.boot_node_ids.clone();
        let connection_limit = Arc::new(Semaphore::new(MAX_TCP_CONNECTIONS));

        loop {
            let (mut stream, peer_addr) = listener.accept().await?;
            let connection_permit = match connection_limit.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    tracing::warn!(
                        %peer_addr,
                        max_connections = MAX_TCP_CONNECTIONS,
                        "[Iron] [cluster] Raft TCP 连接数已达上限，拒绝新连接"
                    );
                    continue;
                }
            };

            let raft = self.raft.clone();
            let boot_node_ids = boot_node_ids.clone();
            tokio::spawn(async move {
                let _connection_permit = connection_permit;
                if let Err(error) = Self::handle_connection(raft, boot_node_ids, &mut stream).await
                {
                    tracing::warn!(%peer_addr, %error, "[Iron] [cluster] 处理 Raft TCP 连接失败");
                }
            });
        }
    }

    // 在单个连接上循环处理多个请求。
    async fn handle_connection(
        raft: Raft<IronRaftTypeConfig>,
        boot_node_ids: BTreeSet<u64>,
        stream: &mut tokio::net::TcpStream,
    ) -> Result<(), std::io::Error> {
        loop {
            let request = match IronRaftTcpFrame::read_json::<IronRaftTcpRpcRequest>(stream).await {
                Ok(request) => request,
                Err(error) => {
                    if IronRaftTcpFrame::is_connection_closed(&error) {
                        return Ok(());
                    }
                    return Err(error);
                }
            };

            let response = match request {
                IronRaftTcpRpcRequest::AppendEntries(rpc) => {
                    let result = raft.append_entries(rpc).await;
                    IronRaftTcpRpcResponse::AppendEntries(result)
                }
                IronRaftTcpRpcRequest::Vote(rpc) => {
                    let result = raft.vote(rpc).await;
                    IronRaftTcpRpcResponse::Vote(result)
                }
                IronRaftTcpRpcRequest::FullSnapshot(request) => {
                    let vote = Self::build_vote(
                        request.vote_term,
                        request.vote_node_id,
                        request.vote_committed,
                    );
                    let meta = Self::build_snapshot_meta(&request.meta);
                    let snapshot = Snapshot {
                        meta,
                        snapshot: Box::new(Cursor::new(request.snapshot)),
                    };
                    let result = raft
                        .install_full_snapshot(vote, snapshot)
                        .await
                        .map(|resp| Self::build_response(resp.vote));
                    IronRaftTcpRpcResponse::FullSnapshot(result)
                }
                IronRaftTcpRpcRequest::ClientWrite(request) => {
                    let (action, key, value) = match &request {
                        IronRaftRequest::ClusterData(command) => match command {
                            crate::cluster_data::iron_cluster_data_command::IronClusterDataCommand::Set {
                                key,
                                value,
                            } => ("set", key.clone(), value.clone()),
                        },
                    };
                    let result = raft
                        .client_write(request)
                        .await
                        .map(|response| response.data)
                        .map_err(|error| error.to_string());
                    if result.is_ok() {
                        tracing::debug!(
                            action,
                            key = %key,
                            value = %value,
                            "[Iron] [cluster-data] leader 收到并写入集群业务数据"
                        );
                    }
                    IronRaftTcpRpcResponse::ClientWrite(result)
                }
                IronRaftTcpRpcRequest::JoinNode {
                    node_id,
                    node_name,
                    node_addr,
                } => {
                    let result = Self::handle_join_node(
                        raft.clone(),
                        boot_node_ids.clone(),
                        node_id,
                        node_name,
                        node_addr,
                    )
                    .await;
                    IronRaftTcpRpcResponse::JoinNode(result)
                }
            };

            IronRaftTcpFrame::write_json(stream, &response).await?;
        }
    }

    // 从请求字段构建投票状态。
    fn build_vote(vote_term: u64, vote_node_id: u64, vote_committed: bool) -> Vote<u64> {
        if vote_committed {
            Vote::new_committed(vote_term, vote_node_id)
        } else {
            Vote::new(vote_term, vote_node_id)
        }
    }

    // 从快照元信息传输模型恢复 SnapshotMeta。
    fn build_snapshot_meta(
        meta: &IronRaftFullSnapshotMeta,
    ) -> SnapshotMeta<u64, openraft::BasicNode> {
        let last_log_id = match (
            meta.last_log_term,
            meta.last_log_node_id,
            meta.last_log_index,
        ) {
            (Some(term), Some(node_id), Some(index)) => Some(openraft::LogId::new(
                CommittedLeaderId::new(term, node_id),
                index,
            )),
            _ => None,
        };

        let voters = meta
            .membership_configs
            .iter()
            .map(|config| config.iter().cloned().collect::<BTreeSet<_>>())
            .collect::<Vec<_>>();
        let nodes = meta
            .membership_nodes
            .iter()
            .map(|node| {
                (
                    node.node_id,
                    openraft::BasicNode::new(node.node_addr.clone()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let membership = Membership::new(voters, nodes);
        let stored_membership = StoredMembership::new(last_log_id.clone(), membership);

        SnapshotMeta {
            last_log_id,
            last_membership: stored_membership,
            snapshot_id: meta.snapshot_id.clone(),
        }
    }

    // 从投票状态构建完整快照响应。
    fn build_response(vote: Vote<u64>) -> IronRaftFullSnapshotResponse {
        IronRaftFullSnapshotResponse {
            vote_term: vote.leader_id.term,
            vote_node_id: vote.leader_id.node_id,
            vote_committed: vote.committed,
        }
    }
}

// IronMesh 自定义扩展协议相关实现。
impl IronRaftTcpServer {
    // 处理节点加入请求。
    async fn handle_join_node(
        raft: Raft<IronRaftTypeConfig>,
        boot_node_ids: BTreeSet<u64>,
        node_id: u64,
        node_name: String,
        node_addr: String,
    ) -> Result<(), String> {
        let peer_tag = peer_node_tag(node_id, &node_name);
        let metrics = raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader {
            return Err(format!(
                "当前节点不是 leader，current_leader={:?}, node_id={}, node_name={}",
                metrics.current_leader, node_id, node_name
            ));
        }

        if metrics
            .membership_config
            .membership()
            .get_node(&node_id)
            .is_some()
        {
            tracing::info!(
                %peer_tag,
                node_addr = %node_addr,
                join_source = "node_join_request",
                "[Iron] [cluster] 节点已经在集群中，跳过重复加入"
            );
            return Ok(());
        }

        tracing::info!(
            %peer_tag,
            node_addr = %node_addr,
            join_source = "node_join_request",
            "[Iron] [cluster] leader 收到节点加入请求"
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
            %peer_tag,
            node_addr = %node_addr,
            join_source = "node_join_request",
            "[Iron] [cluster] leader 已将节点加入集群"
        );
        Ok(())
    }
}
