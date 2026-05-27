use std::sync::Arc;

use openraft::Raft;
use openraft::RaftMetrics;

use crate::api::iron_cluster_write_error::IronClusterWriteError;
use crate::control_plane::iron_cluster_node::IronClusterNode;
use crate::data_plane::iron_cluster_data_command::IronClusterDataCommand;
use crate::raft::iron_raft_constants::CLUSTER_WRITE_RETRY_INTERVAL;
use crate::raft::iron_raft_constants::CLUSTER_WRITE_RETRY_LIMIT;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::model::command::iron_raft_request::IronRaftRequest;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::tcp::iron_raft_tcp_client::IronRaftTcpClient;

// IronMesh 集群写入路由器。
pub(crate) struct IronClusterWriteRouter {
    // 当前集群节点信息，用于判断本地节点是否为 leader。
    current_node: IronClusterNode,
    // Raft 节点句柄，用于本地 leader 写入。
    raft: Raft<IronRaftTypeConfig>,
    // 当前节点到 leader 的业务写入 TCP 客户端缓存。
    leader_write_client: Arc<tokio::sync::Mutex<Option<IronRaftTcpClient>>>,
}

impl IronClusterWriteRouter {
    // 创建集群写入路由器。
    pub(crate) fn new(current_node: IronClusterNode, raft: Raft<IronRaftTypeConfig>) -> Self {
        Self {
            current_node,
            raft,
            leader_write_client: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    // 写入集群业务数据。
    pub(crate) async fn write_cluster_data(
        &self,
        command: IronClusterDataCommand,
    ) -> Result<IronClusterWriteResponse, IronClusterWriteError> {
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
                                let error_kind = error.kind();
                                let error_message = error.to_string();
                                let mut guard = self.leader_write_client.lock().await;
                                if guard.as_ref().is_some_and(|cached_client| {
                                    cached_client.target_node_id == leader_id
                                        && cached_client.target_addr == leader_addr
                                }) {
                                    *guard = None;
                                }
                                match error_kind {
                                    std::io::ErrorKind::TimedOut => {
                                        Err(IronClusterWriteError::ForwardWriteTimeout {
                                            leader_id,
                                            leader_addr,
                                            message: error_message,
                                        })
                                    }
                                    std::io::ErrorKind::InvalidData => {
                                        Err(IronClusterWriteError::ForwardWriteProtocol {
                                            leader_id,
                                            leader_addr,
                                            message: error_message,
                                        })
                                    }
                                    std::io::ErrorKind::Other => {
                                        Err(IronClusterWriteError::ForwardWriteRejected {
                                            leader_id,
                                            leader_addr,
                                            message: error_message,
                                        })
                                    }
                                    _ => Err(IronClusterWriteError::ForwardWriteNetwork {
                                        leader_id,
                                        leader_addr,
                                        message: error_message,
                                    }),
                                }
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
}
