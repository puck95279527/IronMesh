use std::collections::BTreeMap;
use std::io;

use openraft::BasicNode;
use openraft::Raft;
use tokio::net::TcpListener;

use crate::control_plane::IronClusterManagerSupport;
use crate::control_plane::IronClusterNode;
use crate::control_plane::IronClusterNodeRole;
use crate::query::iron_raft_query::start_query_http_with_addr;
use crate::raft::IronTypeConfig;
use crate::raft::network::IronNetworkFactory;
use crate::raft::network::IronTcpServer;
use crate::raft::storage::IronLogStore;
use crate::raft::storage::IronStateMachine;

// IronMesh 集群管理器。
#[derive(Clone, Debug)]
pub struct IronClusterManager {
    pub current_node: IronClusterNode,              // 当前集群节点。
    pub boot_nodes: BTreeMap<u64, IronClusterNode>, // 注册节点表，表内节点会作为投票节点加入集群。
}

impl IronClusterManager {
    // 添加投票节点。
    pub fn add_voter(node_id: u64) -> anyhow::Result<Self> {
        let boot_nodes = IronClusterManagerSupport::load_cluster_boot()?;
        let current_node = boot_nodes.get(&node_id).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("投票节点必须存在于 cluster-boot.toml: node_id={node_id}"),
            )
        })?;

        Ok(Self {
            current_node,
            boot_nodes,
        })
    }

    // 添加学习节点。
    pub fn add_learner(advertise_node_ip: impl Into<String>) -> anyhow::Result<Self> {
        let boot_nodes = IronClusterManagerSupport::load_cluster_boot()?;
        let node_id = boot_nodes
            .keys()
            .next_back()
            .copied()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "注册节点表不能为空"))?
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "学习节点 ID 溢出"))?;

        Ok(Self {
            current_node: IronClusterNode {
                node_id,
                node_ip: advertise_node_ip.into(),
                node_port: None,
                http_debug_addr: None,
                is_boot_node: false,
                node_role: IronClusterNodeRole::Learner,
            },
            boot_nodes,
        })
    }

    // 启动当前集群节点。
    pub async fn start(&self) -> anyhow::Result<()> {
        let config = IronClusterManagerSupport::build_raft_config()?;
        let tcp_listener = TcpListener::bind(self.current_node.bind_addr()).await?;
        let tcp_addr = tcp_listener.local_addr()?;

        let raft = Raft::<IronTypeConfig>::new(
            self.current_node.node_id,
            config,
            IronNetworkFactory::default(),
            IronLogStore::default(),
            IronStateMachine::default(),
        )
        .await?;

        let tcp_server = IronTcpServer::new(raft.clone());
        tokio::spawn(async move {
            if let Err(error) = tcp_server.serve(tcp_listener).await {
                tracing::warn!(%error, "[Iron] [cluster] Raft TCP 服务退出");
            }
        });

        tracing::info!(
            node_id = self.current_node.node_id,
            tcp_addr = %tcp_addr,
            "[Iron] [cluster] Raft TCP 服务已启动"
        );

        if let Some(http_debug_addr) = self.current_node.http_debug_addr.clone() {
            let node_id = self.current_node.node_id;
            let query_raft = raft.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    start_query_http_with_addr(node_id, http_debug_addr, query_raft).await
                {
                    tracing::warn!(%error, "[Iron] [cluster] Raft 查询 HTTP 服务退出");
                }
            });
        }

        if self.current_node.is_boot_node {
            let mut members = BTreeMap::new();
            members.insert(
                self.current_node.node_id,
                BasicNode::new(self.current_node.node_addr()),
            );
            raft.initialize(members).await?;
            tracing::info!(
                node_id = self.current_node.node_id,
                "[Iron] [cluster] 唯一投票节点初始化完成"
            );
        }

        Ok(())
    }
}
