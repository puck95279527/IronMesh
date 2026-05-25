use std::collections::BTreeMap;
use std::error::Error;

use crate::cluster_api::iron_cluster_handle::IronClusterHandle;
use crate::raft::cluster::iron_raft_node::IronRaftNode;
use crate::raft::cluster::manager::iron_raft_cluster_manager_flow::IronRaftClusterManagerFlow;
use crate::raft::cluster::manager::iron_raft_cluster_manager_support::IronRaftClusterManagerSupport;

// IronMesh Raft 集群管理器。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IronRaftClusterManager {
    // 当前 Raft 节点。
    pub current_node: IronRaftNode,
    // 注册节点表，表内节点会作为 Raft voter 加入集群。
    pub boot_nodes: BTreeMap<u64, IronRaftNode>,
}

impl IronRaftClusterManager {
    // 创建 Raft 集群管理器，并从配置文件加载注册节点表。
    pub fn new(mut current_node: IronRaftNode) -> Result<Self, Box<dyn Error>> {
        let boot_nodes = IronRaftClusterManagerSupport::load_cluster_boot()?;
        if let Some(config_node) = boot_nodes.get(&current_node.node_id) {
            current_node.is_boot_node = config_node.is_boot_node;
        }

        Ok(Self {
            current_node,
            boot_nodes,
        })
    }

    // 启动当前节点，等待其完成起盘或加入集群后返回运行句柄。
    pub async fn start(self) -> Result<IronClusterHandle, Box<dyn Error>> {
        // 阶段 1：校验当前节点、注册节点表和唯一首次起盘节点。
        IronRaftClusterManagerFlow::validate_topology(&self)?;

        // 阶段 2：创建 Raft 实例、TCP 服务和本节点运行所需的基础对象。
        let (raft, tcp_server, node_addr, state_machine_store, network_event_receiver) =
            IronRaftClusterManagerFlow::build_raft_runtime(&self).await?;

        // 阶段 3：启动长期运行的后台服务，让节点具备对外通信和调试查询能力。
        let tasks = IronRaftClusterManagerFlow::spawn_runtime_services(
            &self,
            raft.clone(),
            tcp_server,
            node_addr,
            network_event_receiver,
        );

        // 阶段 4：先尝试加入已有集群；只有唯一起盘节点允许初始化新集群。
        let bootstrap_owner =
            IronRaftClusterManagerFlow::bootstrap_or_join_cluster(&self, &raft).await?;

        if bootstrap_owner {
            // 阶段 5：只有首次起盘节点负责把剩余注册节点逐个加入为 voter。
            IronRaftClusterManagerFlow::join_remaining_boot_nodes(&self, &raft).await?;
        }

        Ok(IronClusterHandle::new(
            self.current_node.clone(),
            raft,
            state_machine_store,
            tasks,
        ))
    }

    // 启动当前节点并由调用方显式阻塞等待后台任务。
    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        self.start().await?.wait_forever().await
    }
}
