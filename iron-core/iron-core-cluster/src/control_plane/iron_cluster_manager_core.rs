use std::collections::BTreeMap;
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};

use crate::control_plane::iron_cluster_manager_flow::IronClusterManagerFlow;
use crate::control_plane::iron_cluster_manager_support::IronClusterManagerSupport;
use crate::control_plane::iron_cluster_node::{IronClusterNode, IronClusterNodeRole};
use crate::control_plane::iron_cluster_runtime::IronClusterRuntime;
use crate::utils::iron_snowflake_id::IronSnowflakeIdGenerator;

// IronMesh 集群管理器。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct IronClusterManagerCore {
    // 当前集群节点。
    pub current_node: IronClusterNode,
    // 注册节点表，表内节点会作为投票节点加入集群。
    pub boot_nodes: BTreeMap<u64, IronClusterNode>,
}

impl IronClusterManagerCore {
    // 创建投票节点集群管理器，并从注册节点表按节点 ID 选择当前节点。
    pub(crate) fn add_voter(node_id: u64) -> Result<Self, Box<dyn Error>> {
        let boot_nodes = IronClusterManagerSupport::load_cluster_boot()?;
        let current_node = boot_nodes.get(&node_id).cloned().ok_or_else(|| {
            IoError::new(
                ErrorKind::InvalidInput,
                format!("投票节点必须存在于 cluster-boot.toml: node_id={node_id}"),
            )
        })?;

        Ok(Self {
            current_node,
            boot_nodes,
        })
    }

    // 创建学习节点集群管理器，并从配置文件加载注册节点表。
    pub(crate) fn add_learner(
        advertise_node_ip: impl Into<String>,
    ) -> Result<Self, Box<dyn Error>> {
        let boot_nodes = IronClusterManagerSupport::load_cluster_boot()?;
        let node_id = IronSnowflakeIdGenerator::next_u64();
        if boot_nodes.contains_key(&node_id) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("学习节点不能配置在注册节点表中: node_id={node_id}"),
            )
            .into());
        }

        Ok(Self {
            current_node: IronClusterNode::new(
                node_id,
                advertise_node_ip,
                None,
                None,
                IronClusterNodeRole::Learner,
            ),
            boot_nodes,
        })
    }

    // 启动当前节点，等待其完成起盘或加入集群后返回运行句柄。
    pub(crate) async fn start(mut self) -> Result<IronClusterRuntime, Box<dyn Error>> {
        // 阶段 1：校验当前节点、注册节点表和唯一首次起盘节点。
        IronClusterManagerFlow::validate_topology(&self)?;

        // 阶段 2：先绑定 TCP 端口，再创建 Raft 实例、TCP 服务和本节点运行所需的基础对象。
        let (raft, tcp_server, tcp_listener, state_machine_store, network_event_receiver) =
            IronClusterManagerFlow::build_raft_runtime(&mut self).await?;

        // 阶段 3：启动长期运行的后台服务，让节点在加入集群前已经具备对外通信能力。
        let tasks = IronClusterManagerFlow::spawn_runtime_services(
            &self,
            raft.clone(),
            tcp_server,
            tcp_listener,
            network_event_receiver,
        );

        // 阶段 4：先尝试加入已有集群；只有唯一起盘节点允许初始化新集群。
        let bootstrap_owner =
            IronClusterManagerFlow::bootstrap_or_join_cluster(&self, &raft).await?;

        if bootstrap_owner {
            // 阶段 5：只有首次起盘节点负责把剩余注册节点逐个加入为 voter。
            IronClusterManagerFlow::join_remaining_boot_nodes(&self, &raft).await?;
        }

        Ok(IronClusterRuntime::new(
            self.current_node.clone(),
            raft,
            state_machine_store,
            tasks,
        ))
    }

    // 启动当前节点并由调用方显式阻塞等待后台任务。
    pub(crate) async fn run(self) -> Result<(), Box<dyn Error>> {
        self.start().await?.wait_forever().await
    }
}
