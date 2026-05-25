use std::collections::BTreeMap;
use std::error::Error;

use crate::raft::cluster::iron_raft_node::IronRaftNode;
use crate::raft::cluster::manager::iron_raft_cluster_manager_flow::IronRaftClusterManagerFlow;
use crate::raft::cluster::manager::iron_raft_cluster_manager_support::IronRaftClusterManagerSupport;

// IronMesh Raft 集群管理器。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IronRaftClusterManager {
    // 当前 Raft 节点。
    pub current_node: IronRaftNode,
    // Raft 启动节点表。
    pub boot_nodes: BTreeMap<u64, IronRaftNode>,
}

impl IronRaftClusterManager {
    // 创建 Raft 集群管理器，并从配置文件加载启动节点。
    pub fn new(current_node: IronRaftNode) -> Result<Self, Box<dyn Error>> {
        let boot_nodes = IronRaftClusterManagerSupport::load_cluster_boot()?;
        Ok(Self {
            current_node,
            boot_nodes,
        })
    }

    // 启动当前节点并运行起来。
    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        // 阶段 1：先校验当前节点和启动节点表，确保角色和拓扑关系一致。
        IronRaftClusterManagerFlow::validate_topology(&self)?;

        // 阶段 2：构建 Raft 实例、TCP 服务和本节点运行所需的基础对象。
        let (raft, tcp_server, node_addr) =
            IronRaftClusterManagerFlow::build_raft_runtime(&self).await?;

        // 阶段 3：启动长期运行的后台服务，让节点具备对外通信和调试查询能力。
        IronRaftClusterManagerFlow::spawn_runtime_services(
            &self,
            raft.clone(),
            tcp_server,
            node_addr,
        );

        // 阶段 4：先尝试加入已有集群；如果没有可加入集群，再由 boot 节点争抢起盘。
        let bootstrap_owner =
            IronRaftClusterManagerFlow::bootstrap_or_join_cluster(&self, &raft).await?;

        if bootstrap_owner {
            // 阶段 5：只有起盘节点负责把剩余 boot 节点逐个加入集群。
            IronRaftClusterManagerFlow::join_remaining_boot_nodes(&self, &raft).await?;
        }

        // 阶段 6：启动流程完成后保持进程运行，持续承载 Raft 服务。
        IronRaftClusterManagerFlow::serve_forever().await;
        Ok(())
    }
}
