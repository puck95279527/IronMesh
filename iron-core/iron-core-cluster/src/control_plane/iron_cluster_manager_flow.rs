use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::Raft;
use openraft::ServerState;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::control_plane::IronClusterManager;
use crate::control_plane::IronClusterManagerSupport;
use crate::control_plane::IronClusterNodeRole;
use crate::query::iron_raft_query::start_query_http_with_addr;
use crate::raft::IronTypeConfig;
use crate::raft::network::IronNetworkFactory;
use crate::raft::network::IronTcpClient;
use crate::raft::network::IronTcpServer;
use crate::raft::network::iron_network_factory::IronRaftNetworkEvent;
use crate::raft::storage::IronLogStore;
use crate::raft::storage::IronStateMachine;

const CLUSTER_JOIN_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const LEARNER_REMOVE_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const LEARNER_REMOVE_RETRY_LIMIT: usize = 3;
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

// IronMesh 集群管理启动流程。
#[derive(Clone, Debug, Default)]
pub struct IronClusterManagerFlow;

impl IronClusterManagerFlow {
    // 启动当前集群节点。
    pub async fn start(manager: &IronClusterManager) -> anyhow::Result<()> {
        let mut manager = manager.clone();
        Self::validate_topology(&manager)?;
        let (raft, tcp_server, tcp_listener, network_event_receiver) =
            Self::build_raft_runtime(&mut manager).await?;
        Self::spawn_runtime_services(
            &manager,
            raft.clone(),
            tcp_server,
            tcp_listener,
            network_event_receiver,
        );
        Self::bootstrap_or_join_cluster(&manager, &raft).await?;
        Ok(())
    }

    // 阶段 1：校验当前节点和注册节点表。
    pub fn validate_topology(manager: &IronClusterManager) -> anyhow::Result<()> {
        if manager.boot_nodes.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "注册节点表不能为空").into());
        }

        let boot_node_count = manager
            .boot_nodes
            .values()
            .filter(|node| node.is_boot_node)
            .count();
        if boot_node_count != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "注册节点表中必须且只能配置一个 is_boot_node = true",
            )
            .into());
        }

        if matches!(manager.current_node.node_role, IronClusterNodeRole::Voter)
            && !manager
                .boot_nodes
                .contains_key(&manager.current_node.node_id)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "投票节点必须存在于注册节点表",
            )
            .into());
        }

        tracing::info!(
            node_id = manager.current_node.node_id,
            "[Iron] [cluster] 节点配置校验完成"
        );
        Ok(())
    }

    // 阶段 2：构建 Raft 运行时。
    pub(crate) async fn build_raft_runtime(
        manager: &mut IronClusterManager,
    ) -> anyhow::Result<(
        Raft<IronTypeConfig>,
        IronTcpServer,
        TcpListener,
        mpsc::Receiver<IronRaftNetworkEvent>,
    )> {
        let config = IronClusterManagerSupport::build_raft_config()?;
        let tcp_listener = TcpListener::bind(manager.current_node.bind_addr()).await?;
        let tcp_addr = tcp_listener.local_addr()?;
        manager.current_node.set_resolved_node_port(tcp_addr.port());
        let boot_node_ids = manager.boot_nodes.keys().copied().collect::<BTreeSet<_>>();
        let (network_event_sender, network_event_receiver) = mpsc::channel(1024);

        let raft = Raft::<IronTypeConfig>::new(
            manager.current_node.node_id,
            config,
            IronNetworkFactory::new(network_event_sender),
            IronLogStore::default(),
            IronStateMachine::default(),
        )
        .await?;

        tracing::info!(
            node_id = manager.current_node.node_id,
            tcp_addr = %tcp_addr,
            "[Iron] [cluster] Raft TCP 服务已绑定"
        );

        let tcp_server = IronTcpServer::new(raft.clone(), boot_node_ids);
        Ok((raft, tcp_server, tcp_listener, network_event_receiver))
    }

    // 阶段 3：启动后台运行服务。
    pub(crate) fn spawn_runtime_services(
        manager: &IronClusterManager,
        raft: Raft<IronTypeConfig>,
        tcp_server: IronTcpServer,
        tcp_listener: TcpListener,
        network_event_receiver: mpsc::Receiver<IronRaftNetworkEvent>,
    ) {
        Self::spawn_learner_disconnect_remover(raft.clone(), network_event_receiver);

        tokio::spawn(async move {
            if let Err(error) = tcp_server.serve(tcp_listener).await {
                tracing::warn!(%error, "[Iron] [cluster] Raft TCP 服务退出");
            }
        });

        tracing::info!(
            node_id = manager.current_node.node_id,
            "[Iron] [cluster] Raft TCP 服务已启动"
        );

        if let Some(http_debug_addr) = manager.current_node.http_debug_addr.clone() {
            let node_id = manager.current_node.node_id;
            let query_raft = raft.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    start_query_http_with_addr(node_id, http_debug_addr, query_raft).await
                {
                    tracing::warn!(%error, "[Iron] [cluster] Raft 查询 HTTP 服务退出");
                }
            });
        }
    }

    // 启动 learner TCP 断线自动移除任务。
    fn spawn_learner_disconnect_remover(
        raft: Raft<IronTypeConfig>,
        mut event_receiver: mpsc::Receiver<IronRaftNetworkEvent>,
    ) {
        tokio::spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                Self::handle_raft_network_event(raft.clone(), event).await;
            }
        });
    }

    // 处理 Raft TCP 连接事件。
    async fn handle_raft_network_event(raft: Raft<IronTypeConfig>, event: IronRaftNetworkEvent) {
        tracing::warn!(
            target_node_id = event.target_node_id,
            target_addr = %event.target_addr,
            error = %event.error_message,
            "[Iron] [cluster] learner TCP 复制连接断开，准备移出集群"
        );

        for attempt in 1..=LEARNER_REMOVE_RETRY_LIMIT {
            let (is_leader, is_member, is_voter) = {
                let metrics = raft.metrics().borrow().clone();
                let membership = metrics.membership_config.membership();
                (
                    metrics.state == ServerState::Leader,
                    membership.get_node(&event.target_node_id).is_some(),
                    membership
                        .voter_ids()
                        .any(|node_id| node_id == event.target_node_id),
                )
            };

            if !is_leader {
                return;
            }

            if !is_member {
                return;
            }

            if is_voter {
                return;
            }

            match raft
                .change_membership(
                    ChangeMembers::RemoveNodes(BTreeSet::from([event.target_node_id])),
                    false,
                )
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        target_node_id = event.target_node_id,
                        target_addr = %event.target_addr,
                        attempt,
                        "[Iron] [cluster] learner 已从集群 membership 移除"
                    );
                    return;
                }
                Err(error) if attempt < LEARNER_REMOVE_RETRY_LIMIT => {
                    tracing::warn!(
                        target_node_id = event.target_node_id,
                        target_addr = %event.target_addr,
                        attempt,
                        %error,
                        "[Iron] [cluster] learner 移出集群失败，准备短暂重试"
                    );
                    tokio::time::sleep(LEARNER_REMOVE_RETRY_INTERVAL).await;
                }
                Err(error) => {
                    tracing::warn!(
                        target_node_id = event.target_node_id,
                        target_addr = %event.target_addr,
                        attempt,
                        %error,
                        "[Iron] [cluster] learner 移出集群最终失败"
                    );
                    return;
                }
            }
        }
    }

    // 阶段 4：加入已有集群或初始化最小集群。
    pub async fn bootstrap_or_join_cluster(
        manager: &IronClusterManager,
        raft: &Raft<IronTypeConfig>,
    ) -> anyhow::Result<()> {
        loop {
            if Self::try_join_existing_cluster(manager).await? {
                tracing::info!(
                    node_id = manager.current_node.node_id,
                    "[Iron] [cluster] 当前节点已加入已有集群"
                );
                return Ok(());
            }

            if manager.current_node.is_boot_node {
                Self::initialize_minimal_cluster(manager, raft).await?;
                return Ok(());
            }

            tracing::info!(
                node_id = manager.current_node.node_id,
                "[Iron] [cluster] 当前节点等待加入已有集群"
            );
            tokio::time::sleep(CLUSTER_JOIN_RETRY_INTERVAL).await;
        }
    }

    // 阶段 4.1：尝试加入已有集群。
    pub async fn try_join_existing_cluster(manager: &IronClusterManager) -> anyhow::Result<bool> {
        for (peer_id, peer) in &manager.boot_nodes {
            if *peer_id == manager.current_node.node_id {
                continue;
            }

            let peer_addr = peer.node_addr();
            if !Self::is_peer_reachable(&peer_addr).await {
                continue;
            }

            let client = IronTcpClient::new(peer_addr.clone());
            match client
                .join_cluster(
                    manager.current_node.node_id,
                    manager.current_node.node_addr(),
                )
                .await
            {
                Ok(()) => return Ok(true),
                Err(error) => {
                    tracing::debug!(
                        node_id = manager.current_node.node_id,
                        peer_id,
                        peer_addr = %peer_addr,
                        %error,
                        "[Iron] [cluster] 加入已有集群失败，稍后重试"
                    );
                }
            }
        }

        Ok(false)
    }

    // 阶段 4.2：初始化只包含当前节点的最小集群。
    pub async fn initialize_minimal_cluster(
        manager: &IronClusterManager,
        raft: &Raft<IronTypeConfig>,
    ) -> anyhow::Result<()> {
        let members = BTreeMap::from([(
            manager.current_node.node_id,
            BasicNode::new(manager.current_node.node_addr()),
        )]);
        raft.initialize(members).await?;
        tracing::info!(
            node_id = manager.current_node.node_id,
            "[Iron] [cluster] 最小 Raft 集群初始化完成"
        );
        Ok(())
    }

    // 探测目标节点 TCP 地址是否可达。
    async fn is_peer_reachable(node_addr: &str) -> bool {
        matches!(
            tokio::time::timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(node_addr)).await,
            Ok(Ok(_))
        )
    }
}
