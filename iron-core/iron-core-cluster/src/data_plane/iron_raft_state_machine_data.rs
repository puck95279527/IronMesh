use crate::data_plane::iron_cluster_data::IronClusterData;
use crate::data_plane::iron_cluster_data_command::IronClusterDataCommand;
use crate::raft::model::command::iron_raft_response::IronRaftResponse;

// IronMesh Raft 最小状态机数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronRaftStateMachineData {
    pub cluster_data: IronClusterData, // 状态机中保存的集群业务数据。
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
