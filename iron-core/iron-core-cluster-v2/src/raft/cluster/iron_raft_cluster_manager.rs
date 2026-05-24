use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use openraft::ChangeMembers;
use openraft::Config;
use openraft::Raft;
use openraft::ServerState;

use crate::raft::cluster::iron_raft_boot_node::IronRaftBootNode;
use crate::raft::cluster::iron_raft_node::IronRaftNode;
use crate::logging::{
    bootstrap_node_name,
    many_tag as many_nodes_tag,
    peer_tag as peer_node_tag,
    self_tag as self_node_tag,
};
use crate::http::iron_raft_query::start_query_http_with_addr;
use crate::raft::core::iron_raft_log_store::IronRaftLogStore;
use crate::raft::core::iron_raft_state_machine_store::IronRaftStateMachineStore;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::iron_raft_network_factory::IronRaftNetworkFactory;
use crate::raft::network::iron_raft_tcp_client::IronRaftTcpClient;
use crate::raft::network::iron_raft_tcp_server::IronRaftTcpServer;

// Bootstrap 争抢端口，用来保证同一时刻只有一个节点负责起盘。
const BOOTSTRAP_LOCK_ADDR: &str = "127.0.0.1:4999";

// IronMesh Raft 集群管理器。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IronRaftClusterManager {
    pub current_node: IronRaftNode, // 当前 Raft 节点。
    pub boot_nodes: BTreeMap<u64, IronRaftBootNode>, // Raft 启动节点表。
}

impl IronRaftClusterManager {
    // 创建 Raft 集群管理器。
    pub fn new(
        current_node: IronRaftNode,
        boot_nodes: BTreeMap<u64, IronRaftBootNode>,
    ) -> Self {
        Self {
            current_node,
            boot_nodes,
        }
    }

    // 启动当前节点并跑起来。
    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        let self_tag = self_node_tag(self.current_node.node_id, &self.current_node.node_name);
        let node_id = self.current_node.node_id;
        let node_name = self.current_node.node_name.clone();
        let debug_http_addr = self.current_node.http_debug_addr.clone();

        // 1. 校验当前节点和启动节点表。
        self.validate_topology()?;
        tracing::info!(%self_tag, "[Iron] [cluster] 校验节点配置完成");

        // 2. 创建当前节点的 Raft 实例和 TCP 服务。
        let (raft, tcp_server, node_addr) = self.build_raft_runtime().await?;
        tracing::info!(%self_tag, "[Iron] [cluster] 已创建 Raft 运行时");

        // 3. 启动当前节点的 Raft TCP 服务。
        let _server_task = tokio::spawn(async move {
            if let Err(error) = tcp_server.serve(node_addr).await {
                tracing::warn!(%error, "[Iron] [cluster] Raft TCP 服务退出");
            }
        });

        // 3.1 如果当前节点带有调试 HTTP 地址，就启动查询服务。
        if let Some(http_debug_addr) = debug_http_addr {
            let query_raft = raft.clone();
            tokio::spawn(async move {
                if let Err(error) = start_query_http_with_addr(
                    node_id,
                    node_name,
                    http_debug_addr,
                    query_raft,
                )
                .await
                {
                    tracing::warn!(%error, "[Iron] [cluster] Raft 调试 HTTP 服务退出");
                }
            });
        }

        // 4. 尝试加入已有集群；如果没有已有集群，就争抢起盘资格。
        let bootstrap_owner = self.bootstrap_or_join_cluster(&raft).await?;

        // 5. 如果当前节点是起盘节点，就把其余节点逐个加入集群。
        if bootstrap_owner {
            self.join_remaining_nodes(&raft).await?;
        }

        // 6. 保持服务运行，直到进程退出。
        std::future::pending::<()>().await;
        Ok(())
    }

    // 校验当前节点和启动节点表。
    fn validate_topology(&self) -> Result<(), Box<dyn Error>> {
        if self.boot_nodes.is_empty() {
            return Err(IoError::new(ErrorKind::InvalidInput, "boot_nodes 不能为空").into());
        }

        Ok(())
    }

    // 创建当前节点的 Raft 实例和 TCP 服务。
    async fn build_raft_runtime(&self) -> Result<(Raft<IronRaftTypeConfig>, IronRaftTcpServer, String), Box<dyn Error>> {
        let config = Arc::new(
            Config {
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            }
            .validate()?,
        );

        let node_id = self.current_node.node_id;
        let node_name = self.current_node.node_name.clone();
        let node_addr = self.current_node.node_addr.clone();
        let raft = Raft::<IronRaftTypeConfig>::new(
            node_id,
            config,
            IronRaftNetworkFactory::default(),
            IronRaftLogStore::default(),
            IronRaftStateMachineStore::default(),
        )
        .await?;

        let self_tag = self_node_tag(node_id, &node_name);
        tracing::info!(%self_tag, "[Iron] [cluster] 启动 Raft 集群节点");

        let boot_node_ids = self.boot_nodes.keys().copied().collect::<BTreeSet<_>>();
        Ok((raft.clone(), IronRaftTcpServer::new(raft, boot_node_ids), node_addr))
    }

    // 尝试加入已有集群；如果加入成功，返回 false；如果抢到起盘资格，返回 true。
    async fn bootstrap_or_join_cluster(&self, raft: &Raft<IronRaftTypeConfig>) -> Result<bool, Box<dyn Error>> {
        let self_tag = self_node_tag(self.current_node.node_id, &self.current_node.node_name);
        let many_tag = many_nodes_tag(self.boot_nodes.iter().filter_map(|(peer_id, _)| {
            if *peer_id == self.current_node.node_id {
                None
            } else {
                Some((*peer_id, bootstrap_node_name(*peer_id)))
            }
        }));
        let is_boot_node = self.boot_nodes.contains_key(&self.current_node.node_id);

        tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 开始嗅探 boot 节点");

        loop {
            // 4.1 先尝试请求已有节点，把当前节点加入集群。
            let mut saw_peer = false;
            for (peer_id, peer) in &self.boot_nodes {
                if *peer_id == self.current_node.node_id {
                    continue;
                }

                let peer_tag = peer_node_tag(*peer_id, &bootstrap_node_name(*peer_id));
                let client = IronRaftTcpClient {
                    target_node_id: *peer_id,
                    target_addr: peer.node_addr.clone(),
                    cached_stream: Arc::new(tokio::sync::Mutex::new(None)),
                };

                match client
                    .join_node(
                        self.current_node.node_id,
                        self.current_node.node_name.clone(),
                        self.current_node.node_addr.clone(),
                    )
                    .await
                {
                    Ok(()) => {
                        tracing::info!(%self_tag, %peer_tag, "[Iron] [cluster] 当前节点已加入已有集群");
                        return Ok(false);
                    }
                    Err(error) => {
                        if is_transient_join_error(&error) {
                            continue;
                        }

                        saw_peer = true;
                        tracing::debug!(%self_tag, %peer_tag, %error, "[Iron] [cluster] 已有节点暂时不可加入，稍后重试");
                    }
                }
            }

            // 4.2 如果已经看到别的节点存在，就先等一等，不抢起盘资格。
            if saw_peer {
                tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 已发现可加入的集群节点，稍后重试");
                tokio::time::sleep(Duration::from_millis(800)).await;
                continue;
            }

            // 4.3 没有看到任何已有节点时，尝试抢占 bootstrap 资格。
            if !is_boot_node {
                tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 当前节点不是起盘节点，继续等待集群可加入");
                tokio::time::sleep(Duration::from_millis(800)).await;
                continue;
            }

            tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 未发现可加入的集群节点，准备争抢起盘资格");
            let bootstrap_lock = match tokio::net::TcpListener::bind(BOOTSTRAP_LOCK_ADDR).await {
                Ok(lock) => lock,
                Err(_) => {
                    tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 起盘资格被其他节点占用，稍后重试");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            // 4.4 抢到资格后再确认一遍是否已经有人先起盘。
            tracing::info!(%self_tag, "[Iron] [cluster] 已获得起盘资格");
            let mut saw_peer_after_lock = false;
            for (peer_id, peer) in &self.boot_nodes {
                if *peer_id == self.current_node.node_id {
                    continue;
                }

                let peer_tag = peer_node_tag(*peer_id, &bootstrap_node_name(*peer_id));
                let client = IronRaftTcpClient {
                    target_node_id: *peer_id,
                    target_addr: peer.node_addr.clone(),
                    cached_stream: Arc::new(tokio::sync::Mutex::new(None)),
                };

                match client
                    .join_node(
                        self.current_node.node_id,
                        self.current_node.node_name.clone(),
                        self.current_node.node_addr.clone(),
                    )
                    .await
                {
                    Ok(()) => {
                        tracing::info!(%self_tag, %peer_tag, "[Iron] [cluster] 当前节点已加入已有集群");
                        drop(bootstrap_lock);
                        return Ok(false);
                    }
                    Err(error) => {
                        if is_transient_join_error(&error) {
                            continue;
                        }

                        saw_peer_after_lock = true;
                        tracing::debug!(%self_tag, %peer_tag, %error, "[Iron] [cluster] 已有节点暂时不可加入，稍后重试");
                    }
                }
            }

            if saw_peer_after_lock {
                drop(bootstrap_lock);
                tokio::time::sleep(Duration::from_millis(800)).await;
                continue;
            }

            // 4.5 仍然没有发现已有集群时，把当前节点初始化成最小集群。
            tracing::info!(%self_tag, "[Iron] [cluster] 正在初始化最小集群");
            let init_members = BTreeMap::from([(
                self.current_node.node_id,
                openraft::BasicNode::new(self.current_node.node_addr.clone()),
            )]);

            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Err(error) = raft.initialize(init_members).await {
                drop(bootstrap_lock);
                tracing::warn!(%self_tag, %error, "[Iron] [cluster] 初始化 Raft 集群失败");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            tracing::info!(%self_tag, "[Iron] [cluster] 已完成最小集群初始化");
            drop(bootstrap_lock);
            tracing::info!(%self_tag, "[Iron] [cluster] 已成为起盘节点");
            return Ok(true);
        }
    }

    // 如果当前节点是 bootstrap owner，就把其余节点逐个加入集群。
    async fn join_remaining_nodes(&self, raft: &Raft<IronRaftTypeConfig>) -> Result<(), Box<dyn Error>> {
        let self_tag = self_node_tag(self.current_node.node_id, &self.current_node.node_name);
        let many_tag = many_nodes_tag(self.boot_nodes.iter().filter_map(|(peer_id, _)| {
            if *peer_id == self.current_node.node_id {
                None
            } else {
                Some((*peer_id, bootstrap_node_name(*peer_id)))
            }
        }));

        // 5.1 等待当前节点真正成为 Leader。
        tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 正在等待当前节点成为领导节点");
        if let Err(error) = raft
            .wait(None)
            .state(ServerState::Leader, "等待 bootstrap 节点成为 Leader")
            .await
        {
            return Err(IoError::new(ErrorKind::Other, error.to_string()).into());
        }
        tracing::info!(%self_tag, "[Iron] [cluster] 已确认当前节点成为领导节点");

        // 5.2 依次把其余启动节点加入集群。
        tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 开始逐个加入其他节点");
        let mut did_progress = false;
        for (target_id, target_node) in self.boot_nodes.iter() {
            if *target_id == self.current_node.node_id {
                continue;
            }

            let target_tag = peer_node_tag(*target_id, &bootstrap_node_name(*target_id));
            if !Self::is_peer_reachable(&target_node.node_addr).await {
                tracing::info!(%self_tag, %target_tag, "[Iron] [cluster] 目标节点暂不可达，稍后再试");
                continue;
            }

            let target_basic_node = openraft::BasicNode::new(target_node.node_addr.clone());

            loop {
                match raft.add_learner(*target_id, target_basic_node.clone(), true).await {
                    Ok(_) => break,
                    Err(error) => {
                        tracing::warn!(%self_tag, %target_tag, %error, "[Iron] [cluster] 加入 learner 失败，稍后重试");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            loop {
                match raft
                    .change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([*target_id])), true)
                    .await
                {
                    Ok(_) => break,
                    Err(error) => {
                        tracing::warn!(%self_tag, %target_tag, %error, "[Iron] [cluster] 提升为 voter 失败，稍后重试");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            tracing::info!(%self_tag, %target_tag, "[Iron] [cluster] 节点已逐个加入集群");
            did_progress = true;
        }

        if !did_progress {
            tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 本轮没有可加入的节点，稍后重试");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        tracing::info!(%self_tag, "[Iron] [cluster] 集群启动流程完成");
        Ok(())
    }

    // 探测目标节点的 TCP 地址是否可达。
    async fn is_peer_reachable(node_addr: &str) -> bool {
        matches!(
            tokio::time::timeout(Duration::from_millis(200), tokio::net::TcpStream::connect(node_addr)).await,
            Ok(Ok(_))
        )
    }
}

// 判断节点加入时的错误是不是可以稍后重试。
fn is_transient_join_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::NotConnected
            | ErrorKind::TimedOut
            | ErrorKind::BrokenPipe
            | ErrorKind::UnexpectedEof
    )
}
