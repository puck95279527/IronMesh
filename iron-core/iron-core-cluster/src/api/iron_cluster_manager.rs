use std::error::Error;

use crate::api::iron_cluster_handler::IronClusterHandler;
use crate::raft::control::iron_cluster_manager_core::IronClusterManagerCore;

// 1. IronMesh 集群管理器，是外部调用者启动集群节点的公开入口。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IronClusterManager {
    // 2. 集群内部管理器，封装具体 Raft 控制流程。
    inner: IronClusterManagerCore,
}

impl IronClusterManager {
    // 3. 创建投票节点集群管理器，并从注册节点表按节点 ID 选择当前节点。
    pub fn add_voter(node_id: u64) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: IronClusterManagerCore::add_voter(node_id)?,
        })
    }

    // 4. 创建学习节点集群管理器，并从配置文件加载注册节点表。
    pub fn add_learner(advertise_node_ip: impl Into<String>) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            inner: IronClusterManagerCore::add_learner(advertise_node_ip)?,
        })
    }

    // 5. 启动当前节点，等待其完成起盘或加入集群后返回运行处理器。
    pub async fn start(self) -> Result<IronClusterHandler, Box<dyn Error>> {
        Ok(IronClusterHandler {
            inner: self.inner.start().await?,
        })
    }
}
