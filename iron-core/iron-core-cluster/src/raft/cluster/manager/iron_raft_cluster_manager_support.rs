use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::ChangeMembers;
use openraft::Config;
use openraft::Raft;
use openraft::ServerState;
use tokio::task::JoinSet;
use toml::Value;

use crate::raft::cluster::iron_raft_node::IronRaftNode;
use crate::raft::cluster::iron_raft_node::IronRaftNodeRole;
use crate::raft::cluster::manager::iron_raft_cluster_manager::IronRaftClusterManager;
use crate::raft::iron_raft_constants::BOOT_NODE_JOIN_RETRY_INTERVAL;
use crate::raft::iron_raft_constants::CLUSTER_INITIALIZE_DELAY;
use crate::raft::iron_raft_constants::LEARNER_CLEANUP_INTERVAL;
use crate::raft::iron_raft_constants::LEARNER_CLEANUP_PROBE_COUNT;
use crate::raft::iron_raft_constants::LEARNER_CLEANUP_PROBE_TIMEOUT;
use crate::raft::iron_raft_constants::PEER_REACHABLE_TIMEOUT;
use crate::raft::iron_raft_constants::RAFT_ELECTION_TIMEOUT_MAX;
use crate::raft::iron_raft_constants::RAFT_ELECTION_TIMEOUT_MIN;
use crate::raft::iron_raft_constants::RAFT_HEARTBEAT_INTERVAL;
use crate::raft::iron_raft_constants::VOTER_UNREACHABLE_LOG_INTERVAL;
use crate::raft::iron_raft_log_tag::{peer_tag as peer_node_tag, self_tag as self_node_tag};
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::tcp::iron_raft_tcp_client::IronRaftTcpClient;
use crate::raft::network::tcp::iron_raft_tcp_server::IronRaftTcpServer;
use crate::raft::query::iron_raft_query::start_query_http_with_addr;

// IronMesh Raft 集群管理辅助动作。
pub struct IronRaftClusterManagerSupport;

impl IronRaftClusterManagerSupport {
    // 从 `cluster-boot.toml` 读取注册节点表。
    pub fn load_cluster_boot() -> Result<BTreeMap<u64, IronRaftNode>, Box<dyn Error>> {
        let config_path = env::current_exe()?
            .parent()
            .ok_or_else(|| IoError::new(ErrorKind::NotFound, "无法找到当前可执行文件目录"))?
            .join("cluster-boot.toml");
        let content = fs::read_to_string(&config_path)?;
        let value: Value = toml::from_str(&content)?;
        let boot_nodes_value = value
            .get("IronRaftNode")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("{} 缺少 IronRaftNode 数组", config_path.display()),
                )
            })?;

        let mut boot_nodes = BTreeMap::new();
        for item in boot_nodes_value {
            let table = item.as_table().ok_or_else(|| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("{} 中的 IronRaftNode 条目必须是表", config_path.display()),
                )
            })?;

            let node_id = table
                .get("node_id")
                .and_then(Value::as_integer)
                .ok_or_else(|| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "{} 中的 IronRaftNode 条目缺少 node_id",
                            config_path.display()
                        ),
                    )
                })?;
            if node_id < 0 {
                return Err(
                    IoError::new(ErrorKind::InvalidData, "注册节点 node_id 不能为负数").into(),
                );
            }

            let node_name = table
                .get("node_name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "{} 中的 IronRaftNode 条目缺少 node_name",
                            config_path.display()
                        ),
                    )
                })?;
            let node_addr = table
                .get("node_addr")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "{} 中的 IronRaftNode 条目缺少 node_addr",
                            config_path.display()
                        ),
                    )
                })?;
            let http_debug_addr = table
                .get("http_debug_addr")
                .and_then(Value::as_str)
                .map(|value| value.to_string());
            let is_boot_node = table
                .get("is_boot_node")
                .and_then(Value::as_bool)
                .unwrap_or(false);

            let mut node = IronRaftNode::new(
                node_id as u64,
                node_name,
                node_addr,
                http_debug_addr,
                IronRaftNodeRole::Boot,
            );
            node.is_boot_node = is_boot_node;
            if boot_nodes.insert(node.node_id, node).is_some() {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    "cluster-boot.toml 中存在重复的 node_id",
                )
                .into());
            }
        }

        if boot_nodes.is_empty() {
            return Err(IoError::new(ErrorKind::InvalidData, "cluster-boot.toml 不能为空").into());
        }

        Ok(boot_nodes)
    }

    // 构建并校验 OpenRaft 配置。
    pub fn build_raft_config() -> Result<Arc<Config>, Box<dyn Error>> {
        Ok(Arc::new(
            Config {
                heartbeat_interval: RAFT_HEARTBEAT_INTERVAL,
                election_timeout_min: RAFT_ELECTION_TIMEOUT_MIN,
                election_timeout_max: RAFT_ELECTION_TIMEOUT_MAX,
                ..Default::default()
            }
            .validate()?,
        ))
    }

    // 启动 Raft TCP 服务后台任务。
    pub fn spawn_raft_tcp_server(
        tasks: &mut JoinSet<()>,
        tcp_server: IronRaftTcpServer,
        node_addr: String,
    ) {
        tasks.spawn(async move {
            if let Err(error) = tcp_server.serve(node_addr).await {
                tracing::warn!(%error, "[Iron] [cluster] Raft TCP 服务退出");
            }
        });
    }

    // 启动 leader 清理不可达 learner 节点的后台任务。
    pub(crate) fn spawn_learner_cleanup(tasks: &mut JoinSet<()>, raft: Raft<IronRaftTypeConfig>) {
        tasks.spawn(async move {
            let mut was_leader = false;
            let mut voter_unreachable_log_times = BTreeMap::new();

            loop {
                tokio::time::sleep(LEARNER_CLEANUP_INTERVAL).await;

                let metrics = raft.metrics().borrow().clone();
                if metrics.state != ServerState::Leader {
                    was_leader = false;
                    continue;
                }

                if !was_leader {
                    tracing::info!(
                        node_id = metrics.id,
                        "[Iron] [cluster] 当前节点已成为领导节点(leader)，开始承担集群维护任务"
                    );
                    was_leader = true;
                }

                Self::check_unreachable_voters(&raft, &mut voter_unreachable_log_times).await;
                Self::check_unreachable_learners(&raft).await;
            }
        });
    }

    // 启动可选的调试 HTTP 查询服务后台任务。
    pub fn spawn_debug_http(
        tasks: &mut JoinSet<()>,
        manager: &IronRaftClusterManager,
        raft: Raft<IronRaftTypeConfig>,
    ) {
        let node_id = manager.current_node.node_id;
        let node_name = manager.current_node.node_name.clone();
        let debug_http_addr = manager.current_node.http_debug_addr.clone();

        if let Some(http_debug_addr) = debug_http_addr {
            tasks.spawn(async move {
                if let Err(error) =
                    start_query_http_with_addr(node_id, node_name, http_debug_addr, raft).await
                {
                    tracing::warn!(%error, "[Iron] [cluster] Raft 调试 HTTP 服务退出");
                }
            });
        }
    }

    // 遍历其他注册节点，尝试加入已有集群。
    pub async fn try_join_existing_cluster(
        manager: &IronRaftClusterManager,
    ) -> Result<(bool, bool), Box<dyn Error>> {
        let self_tag = self_node_tag(
            manager.current_node.node_id,
            &manager.current_node.node_name,
        );
        let mut saw_peer = false;

        for (peer_id, peer) in &manager.boot_nodes {
            if *peer_id == manager.current_node.node_id {
                continue;
            }

            let peer_tag = peer_node_tag(*peer_id, peer.node_name.as_str());
            if !Self::is_peer_reachable(&peer.node_addr).await {
                tracing::debug!(%self_tag, %peer_tag, "[Iron] [cluster] 注册节点 TCP 暂不可达，跳过本次加入探测");
                continue;
            }

            let client = IronRaftTcpClient {
                target_node_id: *peer_id,
                target_addr: peer.node_addr.clone(),
                cached_stream: Arc::new(tokio::sync::Mutex::new(None)),
            };

            match client
                .join_node(
                    manager.current_node.node_id,
                    manager.current_node.node_name.clone(),
                    manager.current_node.node_addr.clone(),
                )
                .await
            {
                Ok(()) => {
                    tracing::info!(%self_tag, %peer_tag, "[Iron] [cluster] 当前节点已加入已有集群");
                    return Ok((true, saw_peer));
                }
                Err(error) => {
                    if Self::is_transient_join_error(&error) {
                        continue;
                    }

                    saw_peer = true;
                    tracing::debug!(%self_tag, %peer_tag, %error, "[Iron] [cluster] 起盘节点尚未完成集群初始化，稍后重试");
                }
            }
        }

        Ok((false, saw_peer))
    }

    // 初始化只包含当前节点的最小 Raft 集群。
    pub async fn initialize_minimal_cluster(
        manager: &IronRaftClusterManager,
        raft: &Raft<IronRaftTypeConfig>,
    ) -> Result<(), Box<dyn Error>> {
        let self_tag = self_node_tag(
            manager.current_node.node_id,
            &manager.current_node.node_name,
        );
        tracing::info!(%self_tag, "[Iron] [cluster] 开始初始化最小 Raft 集群");
        let init_members = BTreeMap::from([(
            manager.current_node.node_id,
            openraft::BasicNode::new(manager.current_node.node_addr.clone()),
        )]);

        tokio::time::sleep(CLUSTER_INITIALIZE_DELAY).await;
        raft.initialize(init_members).await?;
        Ok(())
    }

    // 等待当前节点成为领导节点(leader)。
    pub async fn wait_until_leader(
        manager: &IronRaftClusterManager,
        raft: &Raft<IronRaftTypeConfig>,
    ) -> Result<(), Box<dyn Error>> {
        let self_tag = self_node_tag(
            manager.current_node.node_id,
            &manager.current_node.node_name,
        );
        tracing::info!(%self_tag, "[Iron] [cluster] 正在等待当前节点成为领导节点(leader)");
        if let Err(error) = raft
            .wait(None)
            .state(ServerState::Leader, "等待起盘节点成为 leader")
            .await
        {
            return Err(IoError::new(ErrorKind::Other, error.to_string()).into());
        }
        tracing::info!(%self_tag, "[Iron] [cluster] 已确认当前节点成为领导节点(leader)");
        Ok(())
    }

    // 探测目标节点 TCP 地址是否可达。
    pub async fn is_peer_reachable(node_addr: &str) -> bool {
        Self::is_tcp_reachable_with_timeout(node_addr, PEER_REACHABLE_TIMEOUT).await
    }

    // 按指定超时时间探测目标 TCP 地址是否可达。
    async fn is_tcp_reachable_with_timeout(node_addr: &str, timeout: Duration) -> bool {
        matches!(
            tokio::time::timeout(timeout, tokio::net::TcpStream::connect(node_addr)).await,
            Ok(Ok(_))
        )
    }

    // 并发检查不可达的投票节点。
    async fn check_unreachable_voters(
        raft: &Raft<IronRaftTypeConfig>,
        log_times: &mut BTreeMap<u64, Instant>,
    ) {
        let mut checks = JoinSet::new();
        for (node_id, node_addr) in Self::collect_voter_nodes(raft).await {
            checks.spawn(async move {
                let unreachable = Self::confirm_voter_unreachable(&node_addr).await;
                (node_id, node_addr, unreachable)
            });
        }

        while let Some(result) = checks.join_next().await {
            match result {
                Ok((node_id, node_addr, true)) => {
                    if Self::should_log_voter_unreachable(log_times, node_id) {
                        tracing::info!(
                            node_id,
                            node_addr = %node_addr,
                            "[Iron] [cluster] leader 确认投票节点(voter)不可达，保持集群成员关系不变"
                        );
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "[Iron] [cluster] 投票节点(voter)探测任务失败");
                }
            }
        }
    }

    // 并发检查不可达的 learner 节点，并串行执行移除。
    async fn check_unreachable_learners(raft: &Raft<IronRaftTypeConfig>) {
        let mut checks = JoinSet::new();
        for (node_id, node_addr) in Self::collect_learner_nodes(raft).await {
            checks.spawn(async move {
                let unreachable = Self::confirm_learner_unreachable(node_id, &node_addr).await;
                (node_id, node_addr, unreachable)
            });
        }

        let mut unreachable_learners = Vec::new();
        while let Some(result) = checks.join_next().await {
            match result {
                Ok((node_id, node_addr, true)) => unreachable_learners.push((node_id, node_addr)),
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "[Iron] [cluster] learner 节点探测任务失败");
                }
            }
        }

        for (node_id, node_addr) in unreachable_learners {
            Self::remove_unreachable_learner(raft, node_id, node_addr).await;
        }
    }

    // 判断 voter 不可达日志是否已经达到下一次允许记录的时间。
    fn should_log_voter_unreachable(log_times: &mut BTreeMap<u64, Instant>, node_id: u64) -> bool {
        let now = Instant::now();
        match log_times.get(&node_id) {
            Some(last_time) if now.duration_since(*last_time) < VOTER_UNREACHABLE_LOG_INTERVAL => {
                false
            }
            _ => {
                log_times.insert(node_id, now);
                true
            }
        }
    }

    // 收集当前 membership 中的 learner 节点。
    async fn collect_learner_nodes(raft: &Raft<IronRaftTypeConfig>) -> Vec<(u64, String)> {
        let metrics = raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader {
            return Vec::new();
        }

        let voter_ids = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect::<BTreeSet<_>>();

        metrics
            .membership_config
            .nodes()
            .filter_map(|(node_id, node)| {
                if voter_ids.contains(node_id) {
                    None
                } else {
                    Some((*node_id, node.addr.clone()))
                }
            })
            .collect()
    }

    // 收集当前 membership 中除 leader 自身外的投票节点。
    async fn collect_voter_nodes(raft: &Raft<IronRaftTypeConfig>) -> Vec<(u64, String)> {
        let metrics = raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader {
            return Vec::new();
        }

        let self_node_id = metrics.id;
        metrics
            .membership_config
            .membership()
            .voter_ids()
            .filter_map(|node_id| {
                if node_id == self_node_id {
                    None
                } else {
                    metrics
                        .membership_config
                        .membership()
                        .get_node(&node_id)
                        .map(|node| (node_id, node.addr.clone()))
                }
            })
            .collect()
    }

    // 连续嗅探确认 learner 节点是否不可达。
    async fn confirm_learner_unreachable(node_id: u64, node_addr: &str) -> bool {
        if Self::is_tcp_reachable_with_timeout(node_addr, LEARNER_CLEANUP_PROBE_TIMEOUT).await {
            return false;
        }

        tracing::info!(
            node_id,
            node_addr = %node_addr,
            "[Iron] [cluster] leader 发现 learner 节点 TCP 不可达，开始确认"
        );

        for _ in 1..LEARNER_CLEANUP_PROBE_COUNT {
            tokio::time::sleep(LEARNER_CLEANUP_INTERVAL).await;
            if Self::is_tcp_reachable_with_timeout(node_addr, LEARNER_CLEANUP_PROBE_TIMEOUT).await {
                return false;
            }
        }

        true
    }

    // 连续嗅探确认投票节点是否不可达。
    async fn confirm_voter_unreachable(node_addr: &str) -> bool {
        if Self::is_tcp_reachable_with_timeout(node_addr, LEARNER_CLEANUP_PROBE_TIMEOUT).await {
            return false;
        }

        for _ in 1..LEARNER_CLEANUP_PROBE_COUNT {
            tokio::time::sleep(LEARNER_CLEANUP_INTERVAL).await;
            if Self::is_tcp_reachable_with_timeout(node_addr, LEARNER_CLEANUP_PROBE_TIMEOUT).await {
                return false;
            }
        }

        true
    }

    // 移除确认不可达的 learner 节点。
    async fn remove_unreachable_learner(
        raft: &Raft<IronRaftTypeConfig>,
        node_id: u64,
        node_addr: String,
    ) {
        let metrics = raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader {
            return;
        }

        let membership = metrics.membership_config.membership();
        let voter_ids = membership.voter_ids().collect::<BTreeSet<_>>();
        if voter_ids.contains(&node_id) {
            tracing::info!(
                node_id,
                node_addr = %node_addr,
                "[Iron] [cluster] 投票节点(voter)不允许自动移出，已跳过"
            );
            return;
        }

        if membership.get_node(&node_id).is_none() {
            return;
        }

        match raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), true)
            .await
        {
            Ok(_) => {
                tracing::info!(
                    node_id,
                    node_addr = %node_addr,
                    "[Iron] [cluster] leader 已将不可达 learner 节点移出集群"
                );
            }
            Err(error) => {
                tracing::warn!(
                    node_id,
                    node_addr = %node_addr,
                    %error,
                    "[Iron] [cluster] leader 移出不可达 learner 节点失败"
                );
            }
        }
    }

    // 将单个注册节点加入集群。
    pub async fn join_one_boot_node(
        manager: &IronRaftClusterManager,
        raft: &Raft<IronRaftTypeConfig>,
        target_id: u64,
        target_node: &IronRaftNode,
    ) -> Result<bool, Box<dyn Error>> {
        let self_tag = self_node_tag(
            manager.current_node.node_id,
            &manager.current_node.node_name,
        );
        let peer_tag = peer_node_tag(target_id, target_node.node_name.as_str());

        if !Self::is_peer_reachable(&target_node.node_addr).await {
            tracing::info!(
                %self_tag,
                %peer_tag,
                join_source = "leader_boot_scan",
                "[Iron] [cluster] 注册节点暂不可达，本轮跳过"
            );
            return Ok(false);
        }

        let target_basic_node = openraft::BasicNode::new(target_node.node_addr.clone());

        loop {
            match raft
                .add_learner(target_id, target_basic_node.clone(), true)
                .await
            {
                Ok(_) => break,
                Err(error) => {
                    tracing::warn!(%self_tag, %peer_tag, %error, "[Iron] [cluster] 加入 learner 失败，稍后重试");
                    tokio::time::sleep(BOOT_NODE_JOIN_RETRY_INTERVAL).await;
                }
            }
        }

        loop {
            match raft
                .change_membership(
                    ChangeMembers::AddVoterIds(BTreeSet::from([target_id])),
                    true,
                )
                .await
            {
                Ok(_) => break,
                Err(error) => {
                    tracing::warn!(%self_tag, %peer_tag, %error, "[Iron] [cluster] 提升为 voter 失败，稍后重试");
                    tokio::time::sleep(BOOT_NODE_JOIN_RETRY_INTERVAL).await;
                }
            }
        }

        tracing::info!(
            %self_tag,
            %peer_tag,
            join_source = "leader_boot_scan",
            "[Iron] [cluster] leader 已将注册节点加入集群"
        );
        Ok(true)
    }

    // 判断节点加入时的错误是不是可以稍后重试。
    pub fn is_transient_join_error(error: &std::io::Error) -> bool {
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
}
