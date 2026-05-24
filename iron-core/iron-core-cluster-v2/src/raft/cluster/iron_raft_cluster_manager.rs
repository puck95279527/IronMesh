use std::collections::BTreeMap;
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::sync::Arc;

use openraft::Config;
use openraft::Raft;

use crate::raft::cluster::iron_raft_boot_node::IronRaftBootNode;
use crate::raft::cluster::iron_raft_node::IronRaftNode;
use crate::raft::core::iron_raft_log_store::IronRaftLogStore;
use crate::raft::core::iron_raft_state_machine_store::IronRaftStateMachineStore;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::iron_raft_network_factory::IronRaftNetworkFactory;
use crate::raft::network::iron_raft_tcp_server::IronRaftTcpServer;

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
        // 1. 校验当前节点和启动节点表。
        let IronRaftClusterManager {
            current_node,
            boot_nodes,
        } = self;
        if boot_nodes.is_empty() || !boot_nodes.contains_key(&current_node.node_id) {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "当前节点必须存在于 boot_nodes 中",
            )
            .into());
        }

        // 2. 把 boot_nodes 转成 OpenRaft 初始成员。
        let initial_members = boot_nodes
            .iter()
            .map(|(node_id, node)| (*node_id, openraft::BasicNode::new(node.node_addr.clone())))
            .collect::<BTreeMap<_, _>>();

        // 3. 创建当前节点的 Raft 实例。
        let config = Arc::new(
            Config {
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            }
            .validate()?,
        );
        let node_id = current_node.node_id;
        let node_name = current_node.node_name;
        let node_addr = current_node.node_addr;
        let raft = Raft::<IronRaftTypeConfig>::new(
            node_id,
            config,
            IronRaftNetworkFactory::default(),
            IronRaftLogStore::default(),
            IronRaftStateMachineStore::default(),
        )
        .await?;

        let init_raft = raft.clone();
        let tcp_server = IronRaftTcpServer::new(raft);

        tracing::info!(
            node_id = node_id,
            node_name = %node_name,
            node_addr = %node_addr,
            "启动 IronMesh Raft 集群节点"
        );

        // 4. 仅在 1 号节点执行 bootstrap。
        if node_id == 1 {
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                if let Err(error) = init_raft.initialize(initial_members).await {
                    tracing::warn!(%error, "初始化 IronMesh Raft 集群失败");
                }
            });
        }

        // 5. 启动当前节点的 Raft TCP 服务。
        // 6. 保持服务运行，直到进程退出。
        tcp_server.serve(node_addr).await?;
        Ok(())
    }
}
