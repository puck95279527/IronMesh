use openraft::RaftNetworkFactory;

use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::iron_raft_network::IronRaftNetwork;

// IronMesh Raft 网络工厂。
#[derive(Debug, Clone, Default)]
pub struct IronRaftNetworkFactory {}

impl RaftNetworkFactory<IronRaftTypeConfig> for IronRaftNetworkFactory {
    type Network = IronRaftNetwork;

    // 创建目标节点的 HTTP 网络客户端。
    async fn new_client(&mut self, target: u64, node: &openraft::BasicNode) -> Self::Network {
        IronRaftNetwork {
            target_node_id: target,
            target_addr: node.addr.clone(),
        }
    }
}
