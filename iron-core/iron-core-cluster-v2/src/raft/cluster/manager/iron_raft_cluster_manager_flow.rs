use std::collections::BTreeSet;
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::time::Duration;

use openraft::Raft;

use crate::logging::{many_tag as many_nodes_tag, self_tag as self_node_tag};
use crate::raft::cluster::manager::iron_raft_cluster_manager::IronRaftClusterManager;
use crate::raft::cluster::manager::iron_raft_cluster_manager_support::IronRaftClusterManagerSupport;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::iron_raft_network_factory::IronRaftNetworkFactory;
use crate::raft::network::tcp::iron_raft_tcp_server::IronRaftTcpServer;
use crate::raft::storage::iron_raft_log_store::IronRaftLogStore;
use crate::raft::storage::iron_raft_state_machine_store::IronRaftStateMachineStore;

// IronMesh Raft 集群启动主流程。
pub struct IronRaftClusterManagerFlow;

impl IronRaftClusterManagerFlow {
    // 阶段 1：校验当前节点和启动节点表。
    pub fn validate_topology(manager: &IronRaftClusterManager) -> Result<(), Box<dyn Error>> {
        if manager.boot_nodes.is_empty() {
            return Err(IoError::new(ErrorKind::InvalidInput, "boot_nodes 不能为空").into());
        }

        if manager.boot_nodes.values().any(|node| !node.is_boot_node()) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "boot_nodes 中的节点必须都是 Boot 角色",
            )
            .into());
        }

        let contains_current = manager
            .boot_nodes
            .contains_key(&manager.current_node.node_id);
        if contains_current != manager.current_node.is_boot_node() {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "当前节点的角色必须和 boot_nodes 的成员关系一致",
            )
            .into());
        }

        let self_tag = self_node_tag(
            manager.current_node.node_id,
            &manager.current_node.node_name,
        );
        tracing::info!(%self_tag, "[Iron] [cluster] 节点配置校验完成");
        Ok(())
    }

    // 阶段 2：创建当前节点的 Raft 实例和 TCP 服务。
    pub async fn build_raft_runtime(
        manager: &IronRaftClusterManager,
    ) -> Result<(Raft<IronRaftTypeConfig>, IronRaftTcpServer, String), Box<dyn Error>> {
        let config = IronRaftClusterManagerSupport::build_raft_config()?;
        let node_id = manager.current_node.node_id;
        let node_name = manager.current_node.node_name.clone();
        let node_addr = manager.current_node.node_addr.clone();
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
        let boot_node_ids = manager.boot_nodes.keys().copied().collect::<BTreeSet<_>>();
        tracing::info!(%self_tag, "[Iron] [cluster] 已创建 Raft 运行时");
        Ok((
            raft.clone(),
            IronRaftTcpServer::new(raft, boot_node_ids),
            node_addr,
        ))
    }

    // 阶段 3：启动当前节点的后台运行服务。
    pub fn spawn_runtime_services(
        manager: &IronRaftClusterManager,
        raft: Raft<IronRaftTypeConfig>,
        tcp_server: IronRaftTcpServer,
        node_addr: String,
    ) {
        IronRaftClusterManagerSupport::spawn_raft_tcp_server(tcp_server, node_addr);
        IronRaftClusterManagerSupport::spawn_learner_cleanup(raft.clone());
        IronRaftClusterManagerSupport::spawn_debug_http(manager, raft);
    }

    // 阶段 4：尝试加入已有集群，或者争抢起盘资格并初始化最小集群。
    pub async fn bootstrap_or_join_cluster(
        manager: &IronRaftClusterManager,
        raft: &Raft<IronRaftTypeConfig>,
    ) -> Result<bool, Box<dyn Error>> {
        let self_tag = self_node_tag(
            manager.current_node.node_id,
            &manager.current_node.node_name,
        );
        let many_tag = many_nodes_tag(manager.boot_nodes.iter().filter_map(|(peer_id, peer)| {
            if *peer_id == manager.current_node.node_id {
                None
            } else {
                Some((*peer_id, peer.node_name.as_str()))
            }
        }));
        let is_boot_node = manager.current_node.is_boot_node();

        tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 开始尝试 boot 节点流程");

        loop {
            let (joined_existing_cluster, saw_peer) =
                IronRaftClusterManagerSupport::try_join_existing_cluster(manager).await?;
            if joined_existing_cluster {
                return Ok(false);
            }

            if saw_peer {
                tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 已发现可加入的集群节点，稍后重试");
                tokio::time::sleep(Duration::from_millis(800)).await;
                continue;
            }

            if !is_boot_node {
                tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 当前节点不是起盘节点，继续等待集群可加入");
                tokio::time::sleep(Duration::from_millis(800)).await;
                continue;
            }

            tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 未发现已有集群，当前启动节点准备争抢起盘锁");
            let bootstrap_lock = match IronRaftClusterManagerSupport::try_acquire_bootstrap_lock()
                .await
            {
                Some(lock) => lock,
                None => {
                    tracing::info!(%self_tag, %many_tag, "[Iron] [cluster] 起盘资格已被其他节点占用，稍后重试");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            tracing::info!(%self_tag, "[Iron] [cluster] 当前启动节点已获得起盘锁");
            let (joined_existing_cluster, saw_peer_after_lock) =
                IronRaftClusterManagerSupport::try_join_existing_cluster(manager).await?;
            if joined_existing_cluster {
                drop(bootstrap_lock);
                return Ok(false);
            }

            if saw_peer_after_lock {
                drop(bootstrap_lock);
                tokio::time::sleep(Duration::from_millis(800)).await;
                continue;
            }

            if let Err(error) =
                IronRaftClusterManagerSupport::initialize_minimal_cluster(manager, raft).await
            {
                drop(bootstrap_lock);
                tracing::warn!(%self_tag, %error, "[Iron] [cluster] 初始化 Raft 集群失败");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            tracing::info!(%self_tag, "[Iron] [cluster] 最小 Raft 集群初始化完成");
            drop(bootstrap_lock);
            tracing::info!(%self_tag, "[Iron] [cluster] 当前节点已完成起盘");
            return Ok(true);
        }
    }

    // 阶段 5：如果当前节点是起盘节点，就把其他 boot 节点逐个加进集群。
    pub async fn join_remaining_boot_nodes(
        manager: &IronRaftClusterManager,
        raft: &Raft<IronRaftTypeConfig>,
    ) -> Result<(), Box<dyn Error>> {
        let self_tag = self_node_tag(
            manager.current_node.node_id,
            &manager.current_node.node_name,
        );
        let many_tag = many_nodes_tag(manager.boot_nodes.iter().filter_map(|(peer_id, peer)| {
            if *peer_id == manager.current_node.node_id {
                None
            } else {
                Some((*peer_id, peer.node_name.as_str()))
            }
        }));

        IronRaftClusterManagerSupport::wait_until_leader(manager, raft).await?;
        tracing::info!(
            %self_tag,
            %many_tag,
            join_source = "leader_boot_scan",
            "[Iron] [cluster] leader 开始检查 boot 节点加入状态"
        );
        let mut did_progress = false;
        for (target_id, target_node) in manager.boot_nodes.iter() {
            if *target_id == manager.current_node.node_id {
                continue;
            }

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
            tracing::info!(
                %self_tag,
                %many_tag,
                join_source = "leader_boot_scan",
                "[Iron] [cluster] 本轮没有可加入的 boot 节点"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tracing::info!(
            %self_tag,
            join_source = "leader_boot_scan",
            "[Iron] [cluster] 本轮 boot 节点加入检查完成"
        );
        Ok(())
    }

    // 阶段 6：保持服务运行，直到进程退出。
    pub async fn serve_forever() {
        std::future::pending::<()>().await;
    }
}
