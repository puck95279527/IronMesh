use std::error::Error;
use std::marker::PhantomData;

use crate::api::iron_cluster_handler::IronClusterHandler;
use crate::raft::control::iron_cluster_manager_core::IronClusterManagerCore;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// 1. IronMesh 集群管理器，是外部调用者启动集群节点的公开入口。
#[derive(Debug, Clone)]
pub struct IronClusterManager<S>
where
    S: IronRaftStateMachineData,
{
    // 2. 集群内部管理器，封装具体 Raft 控制流程。
    inner: IronClusterManagerCore,
    // 3. 状态机类型标记。
    marker: PhantomData<fn() -> S>,
}

impl<S> IronClusterManager<S>
where
    S: IronRaftStateMachineData,
{
    // 3. 创建投票节点集群管理器，并从注册节点表按节点 ID 选择当前节点。
    pub fn add_voter(node_id: u64) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: IronClusterManagerCore::add_voter(node_id)?,
            marker: PhantomData,
        })
    }

    // 4. 创建学习节点集群管理器，并从配置文件加载注册节点表。
    pub fn add_learner(advertise_node_ip: impl Into<String>) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: IronClusterManagerCore::add_learner(advertise_node_ip)?,
            marker: PhantomData,
        })
    }

    // 5. 启动当前节点，等待其完成起盘或加入集群后返回运行处理器。
    pub async fn start(self) -> Result<IronClusterHandler<S>, Box<dyn Error>> {
        Ok(IronClusterHandler {
            inner: self.inner.start::<S>().await?,
        })
    }
}

impl<S> Eq for IronClusterManager<S> where S: IronRaftStateMachineData {}

impl<S> PartialEq for IronClusterManager<S>
where
    S: IronRaftStateMachineData,
{
    // 比较集群管理器内部配置。
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}
