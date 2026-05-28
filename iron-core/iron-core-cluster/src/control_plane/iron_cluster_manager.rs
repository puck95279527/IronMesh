use std::collections::BTreeMap;
use std::io;

use crate::control_plane::IronClusterManagerFlow;
use crate::control_plane::IronClusterManagerSupport;
use crate::control_plane::IronClusterNode;
use crate::control_plane::IronClusterNodeRole;
use crate::utils::IronSnowflakeIdGenerator;

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
        let node_id = loop {
            let node_id = IronSnowflakeIdGenerator::next_u64();
            if !boot_nodes.contains_key(&node_id) {
                break node_id;
            }
        };

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
        IronClusterManagerFlow::start(self).await
    }
}
