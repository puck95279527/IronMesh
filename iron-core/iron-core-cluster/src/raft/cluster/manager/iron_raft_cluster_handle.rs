use std::error::Error;
use std::io::{Error as IoError, ErrorKind};

use openraft::Raft;
use tokio::task::JoinSet;

use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// IronMesh Raft 集群运行句柄。
pub struct IronRaftClusterHandle {
    // Raft 节点句柄，调用方可以用它读取 metrics 或扩展运行期能力。
    pub raft: Raft<IronRaftTypeConfig>,
    // 当前集群节点托管的后台任务集合。
    tasks: JoinSet<()>,
}

impl IronRaftClusterHandle {
    // 创建 Raft 集群运行句柄。
    pub(crate) fn new(raft: Raft<IronRaftTypeConfig>, tasks: JoinSet<()>) -> Self {
        Self { raft, tasks }
    }

    // 等待后台任务退出，供实际服务进程显式阻塞使用。
    pub async fn wait_forever(mut self) -> Result<(), Box<dyn Error>> {
        match self.tasks.join_next().await {
            Some(Ok(())) => Err(IoError::new(ErrorKind::Other, "Raft 后台任务已退出").into()),
            Some(Err(error)) => Err(IoError::new(
                ErrorKind::Other,
                format!("Raft 后台任务执行失败: {error}"),
            )
            .into()),
            None => Err(IoError::new(ErrorKind::Other, "Raft 后台任务集合为空").into()),
        }
    }
}
