use std::error::Error;

use crate::api::iron_cluster_handler::IronClusterHandler;
use crate::data_plane::iron_cluster_state::IronClusterState;
use crate::raft::control::iron_cluster_manager_core::IronClusterManagerCore;
use crate::raft::storage::iron_raft_state_machine_container::IronRaftStateMachineContainer;

// IronMesh 集群管理器，是外部调用者启动集群节点的公开入口。
#[derive(Debug, Clone)]
pub struct IronClusterManager {
    // 集群内部管理器，封装具体 Raft 控制流程。
    inner: IronClusterManagerCore,
}

impl IronClusterManager {
    // 创建默认数据面状态机的投票节点集群管理器。
    pub fn add_voter(node_id: u64) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: IronClusterManagerCore::add_voter(node_id)?,
        })
    }

    // 创建默认数据面状态机的学习节点集群管理器。
    pub fn add_learner(advertise_node_ip: impl Into<String>) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: IronClusterManagerCore::add_learner(advertise_node_ip)?,
        })
    }

    // 启动当前节点，内部使用 Raft 状态机容器，外部仍返回数据面处理器。
    pub async fn start(self) -> Result<IronClusterHandler, Box<dyn Error>> {
        Ok(IronClusterHandler {
            inner: self
                .inner
                .start::<IronRaftStateMachineContainer<IronClusterState>>()
                .await?,
        })
    }
}
