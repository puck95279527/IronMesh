use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh Raft 状态机总容器。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronRaftStateMachineContainer<D> {
    pub cluster_state: D, // 集群数据面状态。
}

impl<D> IronRaftStateMachineData for IronRaftStateMachineContainer<D>
where
    D: IronRaftStateMachineData,
{
    type WriteRequest = D::WriteRequest;
    type WriteResponse = D::WriteResponse;

    // 应用一条 Raft 写入请求到集群数据面。
    fn apply_raft_request(&mut self, request: Self::WriteRequest) -> Self::WriteResponse {
        self.cluster_state.apply_raft_request(request)
    }
}
