use std::collections::BTreeSet;
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};

use openraft::Raft;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::raft::cluster::iron_raft_node::IronRaftNodeRole;
use crate::raft::cluster::manager::iron_raft_cluster_manager::IronRaftClusterManager;
use crate::raft::cluster::manager::iron_raft_cluster_manager_support::IronRaftClusterManagerSupport;
use crate::raft::iron_raft_constants::BOOT_NODE_JOIN_EMPTY_ROUND_INTERVAL;
use crate::raft::iron_raft_constants::CLUSTER_STARTUP_ERROR_RETRY_INTERVAL;
use crate::raft::iron_raft_constants::CLUSTER_STARTUP_RETRY_INTERVAL;
use crate::raft::iron_raft_log_tag::{other_tag as other_nodes_tag, self_tag as self_node_tag};
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::iron_raft_network_factory::IronRaftNetworkEvent;
use crate::raft::network::iron_raft_network_factory::IronRaftNetworkFactory;
use crate::raft::network::tcp::iron_raft_tcp_server::IronRaftTcpServer;
use crate::raft::storage::iron_raft_log_store::IronRaftLogStore;
use crate::raft::storage::iron_raft_state_machine_store::IronRaftStateMachineStore;

// IronMesh Raft 集群启动主流程。
pub struct IronRaftClusterManagerFlow;

impl IronRaftClusterManagerFlow {
    // 阶段 1：校验当前节点、注册节点表和唯一首次起盘节点，避免后续启动阶段带着错误拓扑进入 Raft。
    pub fn validate_topology(manager: &IronRaftClusterManager) -> Result<(), Box<dyn Error>> {
        // 注册节点表是集群发现入口，voter 和 learner 都依赖它找到已有集群。
        if manager.boot_nodes.is_empty() {
            return Err(IoError::new(ErrorKind::InvalidInput, "注册节点表不能为空").into());
        }

        // 只能有一个首次起盘节点，否则多个节点可能同时 initialize 出彼此独立的 Raft 集群。
        let boot_node_count = manager
            .boot_nodes
            .values()
            .filter(|node| node.is_boot_node())
            .count();
        if boot_node_count != 1 {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "注册节点表中必须且只能配置一个 is_boot_node = true",
            )
            .into());
        }

        // 注册节点必须是稳定入口，端口不能随机；随机端口只允许给 learner 这类扩容节点使用。
        for boot_node in manager.boot_nodes.values() {
            if boot_node.node_port.is_none() {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "注册节点必须配置固定 node_port: node_id={}",
                        boot_node.node_id
                    ),
                )
                .into());
            }
        }

        let contains_current = manager
            .boot_nodes
            .contains_key(&manager.current_node.node_id);
        // voter 必须来自注册表，learner 必须不在注册表中，避免一个节点同时承担两种拓扑语义。
        match manager.current_node.node_role {
            IronRaftNodeRole::Voter if !contains_current => {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    "Raft voter 节点必须存在于 cluster-boot.toml",
                )
                .into());
            }
            IronRaftNodeRole::Learner if contains_current => {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    "Raft learner 节点不能配置在注册节点表中",
                )
                .into());
            }
            _ => {}
        }

        let self_tag = self_node_tag(manager.current_node.node_id);
        tracing::info!(%self_tag, "[Iron] [cluster] 节点配置校验完成");
        Ok(())
    }

    // 阶段 2：先绑定当前节点 TCP 端口，再创建 Raft 实例和 TCP 服务对象，确保后续写入 membership 的地址已经可用。
    pub(crate) async fn build_raft_runtime(
        manager: &mut IronRaftClusterManager,
    ) -> Result<
        (
            Raft<IronRaftTypeConfig>,
            IronRaftTcpServer,
            TcpListener,
            IronRaftStateMachineStore,
            mpsc::Receiver<IronRaftNetworkEvent>,
        ),
        Box<dyn Error>,
    > {
        // 先 bind 才能保证端口属于当前进程；learner 使用 0 端口时，这一步会解析出真实随机端口。
        let bind_addr = manager.current_node.bind_addr();
        let tcp_listener = TcpListener::bind(&bind_addr).await?;
        let local_addr = tcp_listener.local_addr()?;
        manager
            .current_node
            .set_resolved_node_port(local_addr.port());
        let node_addr = manager.current_node.node_addr();

        // Raft 运行时必须使用已经确定的 node_id；网络工厂会在复制失败时把断线事件送回管理流程。
        let config = IronRaftClusterManagerSupport::build_raft_config()?;
        let node_id = manager.current_node.node_id;
        let state_machine_store = IronRaftStateMachineStore::default();
        let (network_event_sender, network_event_receiver) = mpsc::channel(1024);
        let raft = Raft::<IronRaftTypeConfig>::new(
            node_id,
            config,
            IronRaftNetworkFactory::new(network_event_sender),
            IronRaftLogStore::default(),
            state_machine_store.clone(),
        )
        .await?;

        let self_tag = self_node_tag(node_id);
        tracing::info!(%self_tag, %bind_addr, %node_addr, "[Iron] [cluster] 已绑定 Raft TCP 端口");
        tracing::info!(%self_tag, "[Iron] [cluster] 启动 Raft 集群节点");
        // TCP server 需要知道哪些节点是注册节点，收到注册节点 join 时会继续提升为 voter。
        let boot_node_ids = manager.boot_nodes.keys().copied().collect::<BTreeSet<_>>();
        tracing::info!(%self_tag, "[Iron] [cluster] 已创建 Raft 运行时");
        Ok((
            raft.clone(),
            IronRaftTcpServer::new(raft, boot_node_ids),
            tcp_listener,
            state_machine_store,
            network_event_receiver,
        ))
    }

    // 阶段 3：用阶段 2 已经绑定好的 TCP listener 启动当前节点的后台运行服务，确保 join 前节点已经能被连接。
    pub(crate) fn spawn_runtime_services(
        manager: &IronRaftClusterManager,
        raft: Raft<IronRaftTypeConfig>,
        tcp_server: IronRaftTcpServer,
        tcp_listener: TcpListener,
        network_event_receiver: mpsc::Receiver<IronRaftNetworkEvent>,
    ) -> JoinSet<()> {
        let mut tasks = JoinSet::new();
        // Raft TCP 服务必须最先进入后台任务，后续 join 成功后 leader 会立刻尝试复制日志到本节点。
        IronRaftClusterManagerSupport::spawn_raft_tcp_server(&mut tasks, tcp_server, tcp_listener);
        // 调试 HTTP 只用于人工查询，不参与集群控制面决策。
        IronRaftClusterManagerSupport::spawn_debug_http(&mut tasks, manager, raft.clone());
        // 断线移除任务只在当前节点成为 leader 时生效，用来清理不可达 learner。
        IronRaftClusterManagerSupport::spawn_learner_disconnect_remover(
            &mut tasks,
            raft,
            network_event_receiver,
        );
        tasks
    }

    // 阶段 4：先尝试加入已有集群；只有唯一起盘节点允许初始化新集群。
    pub async fn bootstrap_or_join_cluster(
        manager: &IronRaftClusterManager,
        raft: &Raft<IronRaftTypeConfig>,
    ) -> Result<bool, Box<dyn Error>> {
        let self_tag = self_node_tag(manager.current_node.node_id);
        let many_tag = other_nodes_tag(
            manager.current_node.node_id,
            manager.boot_nodes.keys().copied(),
        );
        let is_boot_node = manager.current_node.is_boot_node();

        tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 开始执行集群启动流程");

        loop {
            // 所有节点都先尝试加入已有集群，避免起盘节点重启时误判为需要重新 initialize。
            let (joined_existing_cluster, saw_peer) =
                IronRaftClusterManagerSupport::try_join_existing_cluster(manager, raft).await?;
            if joined_existing_cluster {
                return Ok(false);
            }

            // 非起盘节点只负责等待和重试，绝不主动 initialize，防止形成第二个集群。
            if !is_boot_node {
                if saw_peer {
                    tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 起盘节点尚未完成集群初始化，稍后重试");
                } else {
                    tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 当前节点不是起盘节点，等待起盘节点完成集群初始化");
                }
                tokio::time::sleep(CLUSTER_STARTUP_RETRY_INTERVAL).await;
                continue;
            }

            // 只有唯一的起盘节点在看不到可加入集群时，才允许初始化只包含自己的最小集群。
            tracing::info!(%self_tag, "[Iron] [cluster] 当前节点是起盘节点，准备初始化集群");
            if let Err(error) =
                IronRaftClusterManagerSupport::initialize_minimal_cluster(manager, raft).await
            {
                tracing::warn!(%self_tag, %error, "[Iron] [cluster] 初始化 Raft 集群失败");
                tokio::time::sleep(CLUSTER_STARTUP_ERROR_RETRY_INTERVAL).await;
                continue;
            }

            tracing::info!(%self_tag, "[Iron] [cluster] 最小 Raft 集群初始化完成");
            tracing::info!(%self_tag, "[Iron] [cluster] 当前节点已完成集群起盘");
            return Ok(true);
        }
    }

    // 阶段 5：如果当前节点完成起盘，就把其他注册节点逐个加入为 voter。
    pub async fn join_remaining_boot_nodes(
        manager: &IronRaftClusterManager,
        raft: &Raft<IronRaftTypeConfig>,
    ) -> Result<(), Box<dyn Error>> {
        let self_tag = self_node_tag(manager.current_node.node_id);
        let many_tag = other_nodes_tag(
            manager.current_node.node_id,
            manager.boot_nodes.keys().copied(),
        );

        // 注册节点提升为 voter 前必须确认当前节点已经是 leader，否则 membership 变更会被拒绝。
        IronRaftClusterManagerSupport::wait_until_leader(manager, raft).await?;
        tracing::info!(
            %self_tag,
            %many_tag,
            join_source = "leader_boot_scan",
            "[Iron] [cluster] leader 开始检查注册节点加入状态"
        );
        let mut did_progress = false;
        for (target_id, target_node) in manager.boot_nodes.iter() {
            // 当前节点已经在最小集群中，不需要再次通过 boot scan 加入自己。
            if *target_id == manager.current_node.node_id {
                continue;
            }

            // 每个注册节点先作为 learner 追日志，再提升为 voter，避免未同步节点直接参与投票。
            if IronRaftClusterManagerSupport::join_one_boot_node(
                manager,
                raft,
                *target_id,
                target_node,
            )
            .await?
            {
                did_progress = true;
            }
        }

        if !did_progress {
            // 没有进展时短暂等待，避免起盘节点空转扫描尚未启动的注册节点。
            tracing::info!(
                %self_tag,
                %many_tag,
                join_source = "leader_boot_scan",
                "[Iron] [cluster] 本轮没有可加入的注册节点"
            );
            tokio::time::sleep(BOOT_NODE_JOIN_EMPTY_ROUND_INTERVAL).await;
        }

        tracing::info!(
            %self_tag,
            join_source = "leader_boot_scan",
            "[Iron] [cluster] 本轮注册节点加入检查完成"
        );
        Ok(())
    }
}
