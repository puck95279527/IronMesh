use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::network::RaftNetworkFactory;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::raft::IronTypeConfig;
use crate::raft::network::IronTcpClient;
use crate::raft::network::iron_tcp_client::IronTcpCachedStream;

// IronMesh Raft TCP 连接事件。
#[derive(Clone, Debug)]
pub(crate) enum IronRaftNetworkEvent {
    TargetConnectionFailed {
        target_node_id: u64,   // 断线目标节点标识。
        target_addr: String,   // 断线目标节点 TCP 地址。
        error_message: String, // 触发断线事件的错误信息。
    },
    LocalConnectionClosed {
        peer_addr: String,     // 本地 TCP 连接对端地址。
        error_message: String, // 本地连接关闭原因。
    },
}

// IronMesh Raft TCP 共享连接缓存。
#[derive(Clone, Debug)]
struct IronTcpSharedConnection {
    target_addr: String,                // 目标节点 TCP 地址。
    cached_stream: IronTcpCachedStream, // 目标节点长连接缓存。
}

// IronMesh Raft 网络工厂。
#[derive(Clone, Debug, Default)]
pub struct IronNetworkFactory {
    event_sender: Option<mpsc::Sender<IronRaftNetworkEvent>>, // Raft TCP 连接事件发送器。
    shared_connections: Arc<Mutex<BTreeMap<u64, IronTcpSharedConnection>>>, // Raft TCP 共享连接池。
}

impl IronNetworkFactory {
    // 创建带连接事件发送器的 Raft 网络工厂。
    pub(crate) fn new(event_sender: mpsc::Sender<IronRaftNetworkEvent>) -> Self {
        Self {
            event_sender: Some(event_sender),
            shared_connections: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    // 获取或创建目标节点共享连接缓存。
    async fn get_or_create_cached_stream(
        &self,
        target_node_id: u64,
        target_addr: &str,
    ) -> IronTcpCachedStream {
        let mut guard = self.shared_connections.lock().await;

        if let Some(connection) = guard.get(&target_node_id) {
            if connection.target_addr == target_addr {
                return connection.cached_stream.clone();
            }

            tracing::debug!(
                target_node_id,
                old_target_addr = %connection.target_addr,
                new_target_addr = %target_addr,
                "[Iron] [cluster] Raft TCP 节点地址变化，重建共享连接缓存"
            );
        } else {
            tracing::debug!(
                target_node_id,
                target_addr,
                "[Iron] [cluster] Raft TCP 创建共享连接缓存"
            );
        }

        let cached_stream = Arc::new(Mutex::new(None::<TcpStream>));
        guard.insert(
            target_node_id,
            IronTcpSharedConnection {
                target_addr: target_addr.to_owned(),
                cached_stream: cached_stream.clone(),
            },
        );
        cached_stream
    }

    // 移除指定目标节点的共享连接缓存。
    pub(crate) async fn remove_cached_stream(&self, target_node_id: u64) {
        let mut guard = self.shared_connections.lock().await;
        if guard.remove(&target_node_id).is_some() {
            tracing::debug!(
                target_node_id,
                "[Iron] [cluster] Raft TCP 已移除共享连接缓存"
            );
        }
    }
}

impl RaftNetworkFactory<IronTypeConfig> for IronNetworkFactory {
    type Network = IronTcpClient;

    // 创建目标节点 TCP 客户端。
    async fn new_client(&mut self, target: u64, node: &openraft::BasicNode) -> Self::Network {
        let cached_stream = self.get_or_create_cached_stream(target, &node.addr).await;
        IronTcpClient::new_raft_client(
            target,
            node.addr.clone(),
            cached_stream,
            self.event_sender.clone(),
        )
    }
}
