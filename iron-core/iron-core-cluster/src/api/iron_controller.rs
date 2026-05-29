use crate::api::IronHandler;
use crate::control_plane::IronClusterManager;

// IronMesh 集群控制器。
#[derive(Clone, Debug)]
pub struct IronController {
    pub(crate) cluster_manager: IronClusterManager, // 集群管理器。
}

impl IronController {
    // 添加投票节点。
    pub fn add_voter(node_id: u64) -> anyhow::Result<IronHandler> {
        let cluster_manager = IronClusterManager::add_voter(node_id)?;
        Ok(IronHandler {
            controller: Self { cluster_manager },
            runtime: None,
        })
    }

    // 添加学习节点。
    pub fn add_learner(advertise_node_ip: impl Into<String>) -> anyhow::Result<IronHandler> {
        let cluster_manager = IronClusterManager::add_learner(advertise_node_ip)?;
        Ok(IronHandler {
            controller: Self { cluster_manager },
            runtime: None,
        })
    }
}
