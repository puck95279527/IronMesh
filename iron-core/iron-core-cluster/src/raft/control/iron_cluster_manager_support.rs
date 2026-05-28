use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use openraft::ChangeMembers;
use openraft::Config;
use openraft::Raft;
use openraft::ServerState;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use toml::Value;

use crate::raft::control::iron_cluster_manager_core::IronClusterManagerCore;
use crate::raft::control::iron_cluster_node::IronClusterNode;
use crate::raft::control::iron_cluster_node::IronClusterNodeRole;
use crate::raft::iron_raft_constants::BOOT_NODE_JOIN_RETRY_INTERVAL;
use crate::raft::iron_raft_constants::BOOT_NODE_JOIN_RETRY_LIMIT;
use crate::raft::iron_raft_constants::CLUSTER_INITIALIZE_DELAY;
use crate::raft::iron_raft_constants::JOIN_LOCAL_READY_TIMEOUT;
use crate::raft::iron_raft_constants::LEARNER_REMOVE_RETRY_INTERVAL;
use crate::raft::iron_raft_constants::LEARNER_REMOVE_RETRY_LIMIT;
use crate::raft::iron_raft_constants::PEER_REACHABLE_TIMEOUT;
use crate::raft::iron_raft_constants::RAFT_ELECTION_TIMEOUT_MAX;
use crate::raft::iron_raft_constants::RAFT_ELECTION_TIMEOUT_MIN;
use crate::raft::iron_raft_constants::RAFT_HEARTBEAT_INTERVAL;
use crate::raft::iron_raft_log_tag::{peer_tag as peer_node_tag, self_tag as self_node_tag};
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::iron_raft_network_factory::IronRaftNetworkEvent;
use crate::raft::network::tcp::iron_raft_tcp_client::IronRaftTcpClient;
use crate::raft::network::tcp::iron_raft_tcp_server::IronRaftTcpServer;
use crate::raft::query::iron_raft_query::start_query_http_with_addr;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;
use crate::raft::storage::iron_raft_state_machine_store::IronRaftStateMachineStore;

// IronMesh 集群管理辅助动作。
pub struct IronClusterManagerSupport;

impl IronClusterManagerSupport {
    // 从 `cluster-boot.toml` 读取注册节点表。
    pub fn load_cluster_boot() -> Result<BTreeMap<u64, IronClusterNode>, Box<dyn Error>> {
        let config_path = env::current_exe()?
            .parent()
            .ok_or_else(|| IoError::new(ErrorKind::NotFound, "无法找到当前可执行文件目录"))?
            .join("cluster-boot.toml");
        let content = fs::read_to_string(&config_path)?;
        let value: Value = toml::from_str(&content)?;
        let boot_nodes_value = value
            .get("IronClusterNode")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("{} 缺少 IronClusterNode 数组", config_path.display()),
                )
            })?;

        let mut boot_nodes = BTreeMap::new();
        for item in boot_nodes_value {
            let table = item.as_table().ok_or_else(|| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "{} 中的 IronClusterNode 条目必须是表",
                        config_path.display()
                    ),
                )
            })?;

            let node_id = table
                .get("node_id")
                .and_then(Value::as_integer)
                .ok_or_else(|| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "{} 中的 IronClusterNode 条目缺少 node_id",
                            config_path.display()
                        ),
                    )
                })?;
            if node_id < 0 {
                return Err(
                    IoError::new(ErrorKind::InvalidData, "注册节点 node_id 不能为负数").into(),
                );
            }

            let advertise_node_ip = table
                .get("advertise_node_ip")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "{} 中的 IronClusterNode 条目缺少 advertise_node_ip",
                            config_path.display()
                        ),
                    )
                })?;
            let node_port = table
                .get("node_port")
                .and_then(Value::as_integer)
                .ok_or_else(|| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "{} 中的 IronClusterNode 条目缺少 node_port",
                            config_path.display()
                        ),
                    )
                })?;
            if !(0..=u16::MAX as i64).contains(&node_port) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    "注册节点 node_port 超出 u16 范围",
                )
                .into());
            }
            let http_debug_addr = table
                .get("http_debug_addr")
                .and_then(Value::as_str)
                .map(|value| value.to_string());
            let is_boot_node = table
                .get("is_boot_node")
                .and_then(Value::as_bool)
                .unwrap_or(false);

            let mut node = IronClusterNode::new(
                node_id as u64,
                advertise_node_ip,
                Some(node_port as u16),
                http_debug_addr,
                IronClusterNodeRole::Voter,
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
    pub fn spawn_raft_tcp_server<S>(
        tasks: &mut JoinSet<()>,
        tcp_server: IronRaftTcpServer<S>,
        tcp_listener: TcpListener,
    ) where
        S: IronRaftStateMachineData,
    {
        tasks.spawn(async move {
            if let Err(error) = tcp_server.serve(tcp_listener).await {
                tracing::warn!(%error, "[Iron] [cluster] Raft TCP 服务退出");
            }
        });
    }

    // 启动可选的调试 HTTP 查询服务后台任务。
    pub fn spawn_debug_http<S>(
        tasks: &mut JoinSet<()>,
        manager: &IronClusterManagerCore,
        raft: Raft<IronRaftTypeConfig<S>>,
        state_machine_store: IronRaftStateMachineStore<S>,
    ) where
        S: IronRaftStateMachineData,
    {
        let node_id = manager.current_node.node_id;
        let debug_http_addr = manager.current_node.http_debug_addr.clone();

        if let Some(http_debug_addr) = debug_http_addr {
            tasks.spawn(async move {
                if let Err(error) =
                    start_query_http_with_addr(node_id, http_debug_addr, raft, state_machine_store)
                        .await
                {
                    tracing::warn!(%error, "[Iron] [cluster] Raft 调试 HTTP 服务退出");
                }
            });
        }
    }

    // 启动 learner TCP 断线自动移除任务。
    pub(crate) fn spawn_learner_disconnect_remover<S>(
        tasks: &mut JoinSet<()>,
        raft: Raft<IronRaftTypeConfig<S>>,
        mut event_receiver: mpsc::Receiver<IronRaftNetworkEvent>,
    ) where
        S: IronRaftStateMachineData,
    {
        tasks.spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                Self::handle_raft_network_event(raft.clone(), event).await;
            }
        });
    }

    // 处理 Raft TCP 连接事件。
    async fn handle_raft_network_event<S>(
        raft: Raft<IronRaftTypeConfig<S>>,
        event: IronRaftNetworkEvent,
    ) where
        S: IronRaftStateMachineData,
    {
        tracing::warn!(
            target_node_id = event.target_node_id,
            target_addr = %event.target_addr,
            error = %event.error_message,
            "[Iron] [cluster] learner TCP 复制连接断开，准备移出集群"
        );

        let mut did_try_clean_source_node_data = false;
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
                tracing::debug!(
                    target_node_id = event.target_node_id,
                    target_addr = %event.target_addr,
                    attempt,
                    "[Iron] [cluster] 当前节点不是 leader，停止移除 learner"
                );
                return;
            }

            if !is_member {
                tracing::debug!(
                    target_node_id = event.target_node_id,
                    target_addr = %event.target_addr,
                    attempt,
                    "[Iron] [cluster] 断线节点已不在当前 membership 中"
                );
                return;
            }

            if is_voter {
                tracing::debug!(
                    target_node_id = event.target_node_id,
                    target_addr = %event.target_addr,
                    attempt,
                    "[Iron] [cluster] 断线节点是 voter，保留 membership"
                );
                return;
            }

            if !did_try_clean_source_node_data {
                Self::clean_disconnected_learner_source_node_data(raft.clone(), &event).await;
                did_try_clean_source_node_data = true;
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

    // 清理断线 learner 写入的来源节点数据。
    async fn clean_disconnected_learner_source_node_data<S>(
        raft: Raft<IronRaftTypeConfig<S>>,
        event: &IronRaftNetworkEvent,
    ) where
        S: IronRaftStateMachineData,
    {
        let Some(request) = S::clean_source_node_data_request(event.target_node_id) else {
            tracing::debug!(
                target_node_id = event.target_node_id,
                target_addr = %event.target_addr,
                "[Iron] [cluster-data] 当前状态机未提供来源节点数据清理命令"
            );
            return;
        };

        match raft.client_write(request).await {
            Ok(_) => {
                tracing::info!(
                    target_node_id = event.target_node_id,
                    target_addr = %event.target_addr,
                    "[Iron] [cluster-data] 已提交断线 learner 来源节点数据清理命令"
                );
            }
            Err(error) => {
                tracing::warn!(
                    target_node_id = event.target_node_id,
                    target_addr = %event.target_addr,
                    %error,
                    "[Iron] [cluster-data] 提交断线 learner 来源节点数据清理命令失败"
                );
            }
        }
    }

    // 遍历其他注册节点，尝试加入已有集群。
    pub async fn try_join_existing_cluster<S>(
        manager: &IronClusterManagerCore,
        raft: &Raft<IronRaftTypeConfig<S>>,
    ) -> Result<(bool, bool), Box<dyn Error>>
    where
        S: IronRaftStateMachineData,
    {
        let self_tag = self_node_tag(manager.current_node.node_id);
        let mut saw_peer = false;

        for (peer_id, peer) in &manager.boot_nodes {
            if *peer_id == manager.current_node.node_id {
                continue;
            }

            let peer_tag = peer_node_tag(*peer_id);
            let peer_addr = peer.node_addr();
            if !Self::is_peer_reachable(&peer_addr).await {
                tracing::debug!(%self_tag, %peer_tag, "[Iron] [cluster] 注册节点 TCP 暂不可达，跳过本次加入探测");
                continue;
            }

            let client: IronRaftTcpClient<S> = IronRaftTcpClient {
                target_node_id: *peer_id,
                target_addr: peer_addr,
                cached_stream: Arc::new(tokio::sync::Mutex::new(None)),
                event_sender: None,
                marker: std::marker::PhantomData,
            };

            match client
                .join_node(
                    manager.current_node.node_id,
                    manager.current_node.node_addr(),
                )
                .await
            {
                Ok(()) => {
                    tracing::info!(%self_tag, %peer_tag, "[Iron] [cluster] leader 已接受当前节点加入请求，等待本地 Raft 状态就绪");
                    if let Err(error) =
                        Self::wait_until_local_joined_cluster::<S>(manager, raft, *peer_id).await
                    {
                        saw_peer = true;
                        tracing::warn!(%self_tag, %peer_tag, %error, "[Iron] [cluster] 本地 Raft 状态尚未就绪，稍后重试加入流程");
                        continue;
                    }

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

    // 等待当前节点本地 Raft 状态确认已经完成集群加入。
    pub async fn wait_until_local_joined_cluster<S>(
        manager: &IronClusterManagerCore,
        raft: &Raft<IronRaftTypeConfig<S>>,
        leader_id: u64,
    ) -> Result<(), Box<dyn Error>>
    where
        S: IronRaftStateMachineData,
    {
        let current_node_id = manager.current_node.node_id;
        let self_tag = self_node_tag(manager.current_node.node_id);

        let metrics = raft
            .wait(Some(JOIN_LOCAL_READY_TIMEOUT))
            .metrics(
                move |metrics| {
                    metrics.current_leader == Some(leader_id)
                        && metrics
                            .membership_config
                            .membership()
                            .get_node(&current_node_id)
                            .is_some()
                },
                "等待本地节点感知 leader 并确认 membership",
            )
            .await
            .map_err(|error| IoError::new(ErrorKind::TimedOut, error.to_string()))?;

        tracing::info!(
            %self_tag,
            leader_id,
            membership_log_id = ?metrics.membership_config.log_id(),
            "[Iron] [cluster] 本地 Raft 状态已确认集群加入"
        );
        Ok(())
    }

    // 初始化只包含当前节点的最小 Raft 集群。
    pub async fn initialize_minimal_cluster<S>(
        manager: &IronClusterManagerCore,
        raft: &Raft<IronRaftTypeConfig<S>>,
    ) -> Result<(), Box<dyn Error>>
    where
        S: IronRaftStateMachineData,
    {
        let self_tag = self_node_tag(manager.current_node.node_id);
        tracing::info!(%self_tag, "[Iron] [cluster] 开始初始化最小 Raft 集群");
        let init_members = BTreeMap::from([(
            manager.current_node.node_id,
            openraft::BasicNode::new(manager.current_node.node_addr()),
        )]);

        tokio::time::sleep(CLUSTER_INITIALIZE_DELAY).await;
        raft.initialize(init_members).await?;
        Ok(())
    }

    // 等待当前节点成为领导节点(leader)。
    pub async fn wait_until_leader<S>(
        manager: &IronClusterManagerCore,
        raft: &Raft<IronRaftTypeConfig<S>>,
    ) -> Result<(), Box<dyn Error>>
    where
        S: IronRaftStateMachineData,
    {
        let self_tag = self_node_tag(manager.current_node.node_id);
        tracing::info!(%self_tag, "[Iron] [cluster] 正在等待当前节点成为领导节点(leader)");
        if let Err(error) = raft
            .wait(None)
            .state(ServerState::Leader, "等待起盘节点成为 leader")
            .await
        {
            return Err(IoError::other(error.to_string()).into());
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

    // 将单个注册节点加入集群。
    pub async fn join_one_boot_node<S>(
        manager: &IronClusterManagerCore,
        raft: &Raft<IronRaftTypeConfig<S>>,
        target_id: u64,
        target_node: &IronClusterNode,
    ) -> Result<bool, Box<dyn Error>>
    where
        S: IronRaftStateMachineData,
    {
        let self_tag = self_node_tag(manager.current_node.node_id);
        let peer_tag = peer_node_tag(target_id);

        let target_addr = target_node.node_addr();
        if !Self::is_peer_reachable(&target_addr).await {
            tracing::info!(
                %self_tag,
                %peer_tag,
                join_source = "leader_boot_scan",
                "[Iron] [cluster] 注册节点暂不可达，本轮跳过"
            );
            return Ok(false);
        }

        let target_basic_node = openraft::BasicNode::new(target_addr);

        for attempt in 1..=BOOT_NODE_JOIN_RETRY_LIMIT {
            match raft
                .add_learner(target_id, target_basic_node.clone(), true)
                .await
            {
                Ok(_) => break,
                Err(error) if attempt == BOOT_NODE_JOIN_RETRY_LIMIT => {
                    return Err(IoError::new(
                        ErrorKind::TimedOut,
                        format!("注册节点加入 learner 超过最大重试次数: {error}"),
                    )
                    .into());
                }
                Err(error) => {
                    tracing::warn!(%self_tag, %peer_tag, attempt, %error, "[Iron] [cluster] 加入 learner 失败，稍后重试");
                    tokio::time::sleep(BOOT_NODE_JOIN_RETRY_INTERVAL).await;
                }
            }
        }

        for attempt in 1..=BOOT_NODE_JOIN_RETRY_LIMIT {
            match raft
                .change_membership(
                    ChangeMembers::AddVoterIds(BTreeSet::from([target_id])),
                    true,
                )
                .await
            {
                Ok(_) => break,
                Err(error) if attempt == BOOT_NODE_JOIN_RETRY_LIMIT => {
                    return Err(IoError::new(
                        ErrorKind::TimedOut,
                        format!("注册节点提升为 voter 超过最大重试次数: {error}"),
                    )
                    .into());
                }
                Err(error) => {
                    tracing::warn!(%self_tag, %peer_tag, attempt, %error, "[Iron] [cluster] 提升为 voter 失败，稍后重试");
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
