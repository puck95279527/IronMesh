use openraft::RaftNetworkFactory;
use tokio::sync::Mutex;

use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::tcp::iron_raft_tcp_client::IronRaftTcpClient;

// IronMesh Raft 网络工厂。
#[derive(Debug, Clone, Default)]
pub struct IronRaftNetworkFactory {}

impl RaftNetworkFactory<IronRaftTypeConfig> for IronRaftNetworkFactory {
    type Network = IronRaftTcpClient;

    // 创建目标节点的 TCP 网络客户端。
    async fn new_client(&mut self, target: u64, node: &openraft::BasicNode) -> Self::Network {
        IronRaftTcpClient {
            target_node_id: target,
            target_addr: node.addr.clone(),
            cached_stream: std::sync::Arc::new(Mutex::new(None)),
        }
    }
}
