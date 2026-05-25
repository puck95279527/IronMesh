use openraft::RaftNetworkFactory;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::tcp::iron_raft_tcp_client::IronRaftTcpClient;

// IronMesh Raft TCP 连接事件。
#[derive(Debug, Clone)]
pub(crate) struct IronRaftNetworkEvent {
    pub target_node_id: u64,   // 断线目标节点标识。
    pub target_addr: String,   // 断线目标节点 TCP 地址。
    pub error_message: String, // 触发断线事件的错误信息。
}

// IronMesh Raft 网络工厂。
#[derive(Debug, Clone)]
pub struct IronRaftNetworkFactory {
    event_sender: mpsc::Sender<IronRaftNetworkEvent>, // Raft TCP 连接事件发送器。
}

impl IronRaftNetworkFactory {
    // 创建带连接事件发送器的 Raft 网络工厂。
    pub(crate) fn new(event_sender: mpsc::Sender<IronRaftNetworkEvent>) -> Self {
        Self { event_sender }
    }
}

impl RaftNetworkFactory<IronRaftTypeConfig> for IronRaftNetworkFactory {
    type Network = IronRaftTcpClient;

    // 创建目标节点的 TCP 网络客户端。
    async fn new_client(&mut self, target: u64, node: &openraft::BasicNode) -> Self::Network {
        IronRaftTcpClient {
            target_node_id: target,
            target_addr: node.addr.clone(),
            cached_stream: std::sync::Arc::new(Mutex::new(None)),
            event_sender: Some(self.event_sender.clone()),
        }
    }
}
