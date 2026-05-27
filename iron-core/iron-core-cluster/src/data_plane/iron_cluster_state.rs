use std::collections::BTreeMap;

use crate::data_plane::iron_cluster_data_command::IronClusterDataCommand;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::model::command::iron_raft_request::IronRaftRequest;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh 集群状态数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronClusterState {
    pub values: BTreeMap<String, String>, // 集群业务数据的最小键值存储。
}

impl IronClusterState {
    // 应用集群数据写命令。
    pub(crate) fn apply_cluster_data_command(
        &mut self,
        command: IronClusterDataCommand,
    ) -> IronClusterWriteResponse {
        match command {
            IronClusterDataCommand::Set { key, value } => {
                self.values.insert(key, value.clone());
                IronClusterWriteResponse { value: Some(value) }
            }
        }
    }
}

impl IronRaftStateMachineData for IronClusterState {
    // 应用 Raft 写入请求到默认集群状态机。
    fn apply_raft_request(&mut self, request: IronRaftRequest) -> IronClusterWriteResponse {
        match request {
            IronRaftRequest::ClusterData(command) => self.apply_cluster_data_command(command),
        }
    }
}
