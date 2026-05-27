use std::collections::BTreeMap;

use crate::data_plane::iron_cat::IronCat;
use crate::raft::model::command::iron_cluster_write_request::IronClusterWriteRequest;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh 集群状态数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronClusterState {
    pub values: BTreeMap<u64, IronCat>, // 集群业务数据的最小键值存储。
}

impl IronClusterState {
    // 应用集群数据写命令。
    pub(crate) fn apply_cluster_data_command(
        &mut self,
        request: IronClusterWriteRequest,
    ) -> IronClusterWriteResponse {
        match request {
            IronClusterWriteRequest::Insert(value) => {
                let key = value.id;
                if let Some(existing_value) = self.values.get(&key).cloned() {
                    IronClusterWriteResponse {
                        applied: false,
                        value: Some(existing_value.clone()),
                        previous_value: Some(existing_value),
                        message: Some(format!("键已存在，新增失败: {key}")),
                    }
                } else {
                    self.values.insert(key, value.clone());
                    IronClusterWriteResponse {
                        applied: true,
                        value: Some(value),
                        previous_value: None,
                        message: None,
                    }
                }
            }
            IronClusterWriteRequest::Update(value) => {
                let key = value.id;
                if let Some(previous_value) = self.values.get(&key).cloned() {
                    self.values.insert(key, value.clone());
                    IronClusterWriteResponse {
                        applied: true,
                        value: Some(value),
                        previous_value: Some(previous_value),
                        message: None,
                    }
                } else {
                    IronClusterWriteResponse {
                        applied: false,
                        value: None,
                        previous_value: None,
                        message: Some(format!("键不存在，修改失败: {key}")),
                    }
                }
            }
            IronClusterWriteRequest::Delete(key) => {
                if let Some(previous_value) = self.values.remove(&key) {
                    IronClusterWriteResponse {
                        applied: true,
                        value: None,
                        previous_value: Some(previous_value),
                        message: None,
                    }
                } else {
                    IronClusterWriteResponse {
                        applied: false,
                        value: None,
                        previous_value: None,
                        message: Some(format!("键不存在，删除失败: {key}")),
                    }
                }
            }
        }
    }
}

impl IronRaftStateMachineData for IronClusterState {
    // 应用 Raft 写入请求到默认集群状态机。
    fn apply_raft_request(
        &mut self,
        request: IronClusterWriteRequest,
    ) -> IronClusterWriteResponse {
        self.apply_cluster_data_command(request)
    }
}
