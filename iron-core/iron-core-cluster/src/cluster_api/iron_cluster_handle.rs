use std::error::Error;
use std::io::{Error as IoError, ErrorKind};

use openraft::Raft;
use tokio::task::JoinSet;

use crate::cluster_data::iron_cluster_data_command::IronClusterDataCommand;
use crate::raft::model::command::iron_raft_request::IronRaftRequest;
use crate::raft::model::command::iron_raft_response::IronRaftResponse;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// IronMesh 集群写入错误。
pub type IronClusterWriteError = openraft::error::RaftError<
    u64,
    openraft::error::ClientWriteError<u64, openraft::BasicNode>,
>;

// IronMesh 集群运行句柄。
pub struct IronClusterHandle {
    // Raft 节点句柄，仅供 crate 内部连接底层 Raft 运行时。
    pub(crate) raft: Raft<IronRaftTypeConfig>,
    // 当前集群节点托管的后台任务集合。
    tasks: JoinSet<()>,
}

impl IronClusterHandle {
    // 创建集群运行句柄。
    pub(crate) fn new(raft: Raft<IronRaftTypeConfig>, tasks: JoinSet<()>) -> Self {
        Self { raft, tasks }
    }

    // 写入集群业务数据。
    pub async fn write_cluster_data(
        &self,
        command: IronClusterDataCommand,
    ) -> Result<IronRaftResponse, IronClusterWriteError> {
        let response = self
            .raft
            .client_write(IronRaftRequest::ClusterData(command))
            .await?;

        Ok(response.data)
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
