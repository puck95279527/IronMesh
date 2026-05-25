use std::error::Error;
use std::fmt;
use std::io::{Error as IoError, ErrorKind};
use std::sync::Arc;

use openraft::Raft;
use openraft::RaftMetrics;
use tokio::task::JoinSet;

use crate::cluster_data::iron_cluster_data_command::IronClusterDataCommand;
use crate::raft::cluster::iron_raft_node::IronRaftNode;
use crate::raft::iron_raft_constants::CLUSTER_WRITE_RETRY_INTERVAL;
use crate::raft::iron_raft_constants::CLUSTER_WRITE_RETRY_LIMIT;
use crate::raft::model::command::iron_raft_request::IronRaftRequest;
use crate::raft::model::command::iron_raft_response::IronRaftResponse;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::model::state_machine::iron_raft_state_machine_data::IronRaftStateMachineData;
use crate::raft::network::tcp::iron_raft_tcp_client::IronRaftTcpClient;
use crate::raft::storage::iron_raft_state_machine_store::IronRaftStateMachineStore;

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
                write!(
                    formatter,
                    "当前集群缺少 leader 节点地址 leader_id={leader_id}"
                )
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
    // 当前节点本地状态机存储，用于读取已经 apply 的本地数据。
    state_machine_store: IronRaftStateMachineStore,
    // 当前节点到 leader 的业务写入 TCP 客户端缓存。
    leader_write_client: Arc<tokio::sync::Mutex<Option<IronRaftTcpClient>>>,
    // 当前集群节点托管的后台任务集合。
    tasks: Arc<tokio::sync::Mutex<JoinSet<()>>>,
}

impl IronClusterHandle {
    // 创建集群运行句柄。
    pub(crate) fn new(
        current_node: IronRaftNode,
        raft: Raft<IronRaftTypeConfig>,
        state_machine_store: IronRaftStateMachineStore,
        tasks: JoinSet<()>,
    ) -> Self {
        Self {
            current_node,
            raft,
            state_machine_store,
            leader_write_client: Arc::new(tokio::sync::Mutex::new(None)),
            tasks: Arc::new(tokio::sync::Mutex::new(tasks)),
        }
    }

    // 读取当前节点本地已经 apply 的状态机数据。
    pub async fn local_state_machine_data(&self) -> IronRaftStateMachineData {
        self.state_machine_store.state_machine.lock().await.clone()
    }

    // 写入集群业务数据。
    pub async fn write_cluster_data(
        &self,
        command: IronClusterDataCommand,
    ) -> Result<IronRaftResponse, IronClusterWriteError> {
        let (action, key, value) = match &command {
            IronClusterDataCommand::Set { key, value } => ("set", key.clone(), value.clone()),
        };
        let mut last_error = None;

        for attempt in 1..=CLUSTER_WRITE_RETRY_LIMIT {
            let command = command.clone();
            let metrics = self.raft.metrics().borrow().clone();

            let result = if metrics.current_leader == Some(self.current_node.node_id) {
                match self
                    .raft
                    .client_write(IronRaftRequest::ClusterData(command))
                    .await
                {
                    Ok(response) => Ok((
                        response.data,
                        "local_leader",
                        "not_used",
                        self.current_node.node_id,
                    )),
                    Err(error) => Err(IronClusterWriteError::LocalWrite(error)),
                }
            } else {
                match Self::find_leader_node(&metrics) {
                    Ok((leader_id, leader_addr)) => {
                        let (client, connection_state) =
                            self.leader_write_client(leader_id, &leader_addr).await;
                        match client
                            .client_write(IronRaftRequest::ClusterData(command))
                            .await
                        {
                            Ok(response) => {
                                Ok((response, "forward_to_leader", connection_state, leader_id))
                            }
                            Err(error) => {
                                let mut guard = self.leader_write_client.lock().await;
                                if guard.as_ref().is_some_and(|cached_client| {
                                    cached_client.target_node_id == leader_id
                                        && cached_client.target_addr == leader_addr
                                }) {
                                    *guard = None;
                                }
                                Err(IronClusterWriteError::ForwardWrite(error))
                            }
                        }
                    }
                    Err(error) => Err(error),
                }
            };

            match result {
                Ok((response, write_path, connection_state, leader_id)) => {
                    tracing::debug!(
                        write_path,
                        connection_state,
                        leader_id,
                        action,
                        key = %key,
                        value = %value,
                        attempt,
                        "[Iron] [cluster-data] 集群业务数据写入成功"
                    );

                    return Ok(response);
                }
                Err(error) if attempt < CLUSTER_WRITE_RETRY_LIMIT => {
                    tracing::warn!(
                        action,
                        key = %key,
                        value = %value,
                        attempt,
                        %error,
                        "[Iron] [cluster-data] 集群业务数据写入失败，准备重新读取 leader 后重试"
                    );
                    last_error = Some(error);
                    tokio::time::sleep(CLUSTER_WRITE_RETRY_INTERVAL).await;
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
        }

        Err(last_error.unwrap_or(IronClusterWriteError::NoLeader))
    }

    // 获取当前 leader 的业务写入 TCP 客户端。
    async fn leader_write_client(
        &self,
        leader_id: u64,
        leader_addr: &str,
    ) -> (IronRaftTcpClient, &'static str) {
        let mut guard = self.leader_write_client.lock().await;
        match guard.as_ref() {
            Some(client)
                if client.target_node_id == leader_id && client.target_addr == leader_addr =>
            {
                (client.clone(), "cached")
            }
            _ => {
                let connection_state = if guard.is_some() { "replaced" } else { "new" };
                let client = IronRaftTcpClient {
                    target_node_id: leader_id,
                    target_addr: leader_addr.to_string(),
                    cached_stream: Arc::new(tokio::sync::Mutex::new(None)),
                    event_sender: None,
                };
                *guard = Some(client.clone());
                (client, connection_state)
            }
        }
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
    pub async fn wait_forever(&self) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().await;
        match tasks.join_next().await {
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
