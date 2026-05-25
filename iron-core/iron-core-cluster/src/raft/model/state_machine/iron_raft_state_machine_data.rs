use crate::raft::model::command::iron_raft_response::IronRaftResponse;
use crate::raft::model::state_machine::iron_cluster_data::IronClusterData;
use crate::raft::model::state_machine::iron_cluster_data_command::IronClusterDataCommand;

// IronMesh Raft 最小状态机数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronRaftStateMachineData {
    pub(crate) cluster_data: IronClusterData, // 状态机中保存的集群业务数据。
}

impl IronRaftStateMachineData {
    // 应用集群数据写命令。
    pub(crate) fn apply_cluster_data_command(
        &mut self,
        command: IronClusterDataCommand,
    ) -> IronRaftResponse {
        self.cluster_data.apply_command(command)
    }
}
