use crate::data_plane::iron_cluster_data::IronClusterData;
use crate::data_plane::iron_cluster_data_command::IronClusterDataCommand;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;

// IronMesh 集群状态数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronClusterState {
    pub cluster_data: IronClusterData, // 状态机中保存的集群业务数据。
}

impl IronClusterState {
    // 应用集群数据写命令。
    pub(crate) fn apply_cluster_data_command(
        &mut self,
        command: IronClusterDataCommand,
    ) -> IronClusterWriteResponse {
        self.cluster_data.apply_command(command)
    }
}
