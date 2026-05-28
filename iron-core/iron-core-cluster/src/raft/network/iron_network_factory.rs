use openraft::network::RaftNetworkFactory;

use crate::raft::IronTypeConfig;
use crate::raft::network::IronTcpClient;

// IronMesh Raft 网络工厂。
#[derive(Clone, Debug, Default)]
pub struct IronNetworkFactory;

impl RaftNetworkFactory<IronTypeConfig> for IronNetworkFactory {
    type Network = IronTcpClient;

    // 创建目标节点 TCP 客户端。
    async fn new_client(&mut self, _target: u64, node: &openraft::BasicNode) -> Self::Network {
        IronTcpClient::new(node.addr.clone())
    }
}
