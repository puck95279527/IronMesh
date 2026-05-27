use openraft::RaftNetworkFactory;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::tcp::iron_raft_tcp_client::IronRaftTcpClient;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh Raft TCP 连接事件。
#[derive(Debug, Clone)]
pub(crate) struct IronRaftNetworkEvent {
    pub target_node_id: u64,   // 断线目标节点标识。
    pub target_addr: String,   // 断线目标节点 TCP 地址。
    pub error_message: String, // 触发断线事件的错误信息。
}

// IronMesh Raft 网络工厂。
#[derive(Debug, Clone)]
pub struct IronRaftNetworkFactory<S>
where
    S: IronRaftStateMachineData,
{
    event_sender: mpsc::Sender<IronRaftNetworkEvent>, // Raft TCP 连接事件发送器。
    marker: std::marker::PhantomData<fn() -> S>,      // 状态机类型标记。
}

impl<S> IronRaftNetworkFactory<S>
where
    S: IronRaftStateMachineData,
{
    // 创建带连接事件发送器的 Raft 网络工厂。
    pub(crate) fn new(event_sender: mpsc::Sender<IronRaftNetworkEvent>) -> Self {
        Self {
            event_sender,
            marker: std::marker::PhantomData,
        }
    }
}

impl<S> RaftNetworkFactory<IronRaftTypeConfig<S>> for IronRaftNetworkFactory<S>
where
    S: IronRaftStateMachineData,
{
    type Network = IronRaftTcpClient<S>;

    // 创建目标节点的 TCP 网络客户端。
    async fn new_client(&mut self, target: u64, node: &openraft::BasicNode) -> Self::Network {
        IronRaftTcpClient {
            target_node_id: target,
            target_addr: node.addr.clone(),
            cached_stream: std::sync::Arc::new(Mutex::new(None)),
            event_sender: Some(self.event_sender.clone()),
            marker: std::marker::PhantomData,
        }
    }
}
