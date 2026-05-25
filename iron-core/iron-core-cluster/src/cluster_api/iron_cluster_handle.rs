use std::error::Error;
use std::fmt;
use std::io::{Error as IoError, ErrorKind};
use std::sync::Arc;

use openraft::Raft;
use openraft::RaftMetrics;
use tokio::task::JoinSet;

use crate::cluster_data::iron_cluster_data_command::IronClusterDataCommand;
use crate::raft::cluster::iron_raft_node::IronRaftNode;
use crate::raft::model::command::iron_raft_request::IronRaftRequest;
use crate::raft::model::command::iron_raft_response::IronRaftResponse;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::tcp::iron_raft_tcp_client::IronRaftTcpClient;

// IronMesh 集群写入错误。
#[derive(Debug)]
pub enum IronClusterWriteError {
    // 当前集群暂时没有 leader。
    NoLeader,
    // 当前节点知道 leader 标识，但成员关系中缺少 leader 节点地址。
    LeaderNodeMissing {
        leader_id: u64, // 缺少地址的 leader 节点标识。
    },
    // leader 本地写入失败。
    LocalWrite(
        openraft::error::RaftError<
            u64,
            openraft::error::ClientWriteError<u64, openraft::BasicNode>,
        >,
    ),
    // 非 leader 向 leader 转发写入失败。
    ForwardWrite(std::io::Error),
}

impl fmt::Display for IronClusterWriteError {
    // 格式化集群写入错误。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoLeader => write!(formatter, "当前集群暂时没有 leader"),
            Self::LeaderNodeMissing { leader_id } => {
                write!(formatter, "当前集群缺少 leader 节点地址 leader_id={leader_id}")
            }
            Self::LocalWrite(error) => write!(formatter, "leader 本地写入失败: {error}"),
            Self::ForwardWrite(error) => write!(formatter, "转发 leader 写入失败: {error}"),
        }
    }
}

impl Error for IronClusterWriteError {
    // 返回底层错误来源。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LocalWrite(error) => Some(error),
            Self::ForwardWrite(error) => Some(error),
            _ => None,
        }
    }
}

// IronMesh 集群运行句柄。
pub struct IronClusterHandle {
    // 当前集群节点信息，用于对外 API 记录操作来源。
    current_node: IronRaftNode,
    // Raft 节点句柄，仅供 crate 内部连接底层 Raft 运行时。
    pub(crate) raft: Raft<IronRaftTypeConfig>,
    // 当前集群节点托管的后台任务集合。
    tasks: JoinSet<()>,
}

impl IronClusterHandle {
    // 创建集群运行句柄。
    pub(crate) fn new(
        current_node: IronRaftNode,
        raft: Raft<IronRaftTypeConfig>,
        tasks: JoinSet<()>,
    ) -> Self {
        Self {
            current_node,
            raft,
            tasks,
        }
    }

    // 写入集群业务数据。
    pub async fn write_cluster_data(
        &self,
        command: IronClusterDataCommand,
    ) -> Result<IronRaftResponse, IronClusterWriteError> {
        let (action, key, value) = match &command {
            IronClusterDataCommand::Set { key, value } => {
                ("set", key.clone(), value.clone())
            }
        };
        let metrics = self.raft.metrics().borrow().clone();

        let (response, write_path, leader_id, leader_addr) =
            if metrics.current_leader == Some(self.current_node.node_id) {
                let response = self
                    .raft
                    .client_write(IronRaftRequest::ClusterData(command))
                    .await
                    .map_err(IronClusterWriteError::LocalWrite)?;
                (
                    response.data,
                    "local_leader",
                    self.current_node.node_id,
                    self.current_node.node_addr.clone(),
                )
            } else {
                let (leader_id, leader_addr) = Self::find_leader_node(&metrics)?;
                let client = IronRaftTcpClient {
                    target_node_id: leader_id,
                    target_addr: leader_addr.clone(),
                    cached_stream: Arc::new(tokio::sync::Mutex::new(None)),
                };
                let response = client
                    .client_write(IronRaftRequest::ClusterData(command))
                    .await
                    .map_err(IronClusterWriteError::ForwardWrite)?;
                (response, "forward_to_leader", leader_id, leader_addr)
            };

        tracing::debug!(
            node_id = self.current_node.node_id,
            node_name = %self.current_node.node_name,
            node_addr = %self.current_node.node_addr,
            write_path,
            leader_id,
            leader_addr = %leader_addr,
            action,
            key = %key,
            value = %value,
            "[Iron] [cluster-data] 集群业务数据写入成功"
        );

        Ok(response)
    }

    // 从 Raft 指标中查找当前 leader 节点。
    fn find_leader_node(
        metrics: &RaftMetrics<u64, openraft::BasicNode>,
    ) -> Result<(u64, String), IronClusterWriteError> {
        let leader_id = metrics
            .current_leader
            .ok_or(IronClusterWriteError::NoLeader)?;
        let leader_node = metrics
            .membership_config
            .membership()
            .get_node(&leader_id)
            .ok_or(IronClusterWriteError::LeaderNodeMissing { leader_id })?;

        Ok((leader_id, leader_node.addr.clone()))
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
