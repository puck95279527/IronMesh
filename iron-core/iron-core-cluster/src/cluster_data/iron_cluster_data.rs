use std::collections::BTreeMap;

use crate::cluster_data::iron_cluster_data_command::IronClusterDataCommand;
use crate::raft::model::command::iron_raft_response::IronRaftResponse;

// IronMesh 集群业务数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronClusterData {
    pub values: BTreeMap<String, String>, // 集群业务数据的最小键值存储。
}

impl IronClusterData {
    // 应用集群数据写命令。
    pub(crate) fn apply_command(&mut self, command: IronClusterDataCommand) -> IronRaftResponse {
        match command {
            IronClusterDataCommand::Set { key, value } => {
                self.values.insert(key, value.clone());
                IronRaftResponse { value: Some(value) }
            }
        }
    }
}
