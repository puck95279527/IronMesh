use openraft::network::RaftNetworkFactory;
use tokio::sync::mpsc;

use crate::raft::IronTypeConfig;
use crate::raft::network::IronTcpClient;

// IronMesh Raft TCP 连接事件。
#[derive(Clone, Debug)]
pub(crate) struct IronRaftNetworkEvent {
    pub target_node_id: u64,   // 断线目标节点标识。
    pub target_addr: String,   // 断线目标节点 TCP 地址。
    pub error_message: String, // 触发断线事件的错误信息。
}

// IronMesh Raft 网络工厂。
#[derive(Clone, Debug, Default)]
pub struct IronNetworkFactory {
    event_sender: Option<mpsc::Sender<IronRaftNetworkEvent>>, // Raft TCP 连接事件发送器。
}

impl IronNetworkFactory {
    // 创建带连接事件发送器的 Raft 网络工厂。
    pub(crate) fn new(event_sender: mpsc::Sender<IronRaftNetworkEvent>) -> Self {
        Self {
            event_sender: Some(event_sender),
        }
    }
}

impl RaftNetworkFactory<IronTypeConfig> for IronNetworkFactory {
    type Network = IronTcpClient;

    // 创建目标节点 TCP 客户端。
    async fn new_client(&mut self, target: u64, node: &openraft::BasicNode) -> Self::Network {
        IronTcpClient::new_raft_client(target, node.addr.clone(), self.event_sender.clone())
    }
}
