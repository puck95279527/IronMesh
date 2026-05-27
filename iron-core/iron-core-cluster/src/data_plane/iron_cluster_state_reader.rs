use crate::data_plane::iron_cluster_state::IronClusterState;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;
use crate::raft::storage::iron_raft_state_machine_store::IronRaftStateMachineStore;

// IronMesh 集群状态读取器。
#[derive(Debug, Clone)]
pub(crate) struct IronClusterStateReader<S = IronClusterState>
where
    S: IronRaftStateMachineData,
{
    // 当前节点本地状态机存储。
    state_machine_store: IronRaftStateMachineStore<S>,
}

impl<S> IronClusterStateReader<S>
where
    S: IronRaftStateMachineData,
{
    // 创建集群状态读取器。
    pub(crate) fn new(state_machine_store: IronRaftStateMachineStore<S>) -> Self {
        Self {
            state_machine_store,
        }
    }

    // 读取当前节点本地已经 apply 的状态机数据。
    pub(crate) async fn local_state_machine_data(&self) -> S {
        self.state_machine_store.state_machine.lock().await.clone()
    }
}
