use std::collections::BTreeMap;

use crate::data_plane::iron_cluster_entity::IronClusterEntity;
use crate::data_plane::iron_cluster_entity::IronClusterEntityModel;
use crate::data_plane::model::iron_cat::IronCat;
use crate::data_plane::model::iron_dog::IronDog;
use crate::raft::model::command::iron_cluster_write_request::IronClusterWriteRequest;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh 集群状态数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronClusterState {
    pub cats: BTreeMap<u64, IronCat>, // 集群猫数据的最小键值存储。
    pub dogs: BTreeMap<u64, IronDog>, // 集群狗数据的最小键值存储。
}

impl IronRaftStateMachineData for IronClusterState {
    type WriteRequest = IronClusterWriteRequest<IronClusterEntity>;
    type WriteResponse = IronClusterWriteResponse<IronClusterEntity>;

    // 应用 Raft 写入请求到默认集群状态机。
    fn apply_raft_request(&mut self, request: Self::WriteRequest) -> Self::WriteResponse {
        match request {
            IronClusterWriteRequest::Insert(value) => self.insert_entity(value),
            IronClusterWriteRequest::Update(value) => self.update_entity(value),
            IronClusterWriteRequest::Delete(value) => self.delete_entity(value),
        }
    }
}

impl IronClusterState {
    // 新增集群数据实体。
    fn insert_entity(
        &mut self,
        value: IronClusterEntity,
    ) -> IronClusterWriteResponse<IronClusterEntity> {
        match value {
            IronClusterEntity::Cat(value) => {
                let key = value.entity_key();
                if let Some(existing_value) = self.cats.get(&key).cloned() {
                    IronClusterWriteResponse {
                        applied: false,
                        value: Some(IronClusterEntity::Cat(existing_value.clone())),
                        previous_value: Some(IronClusterEntity::Cat(existing_value)),
                        message: Some(format!("猫数据键已存在，新增失败: {key}")),
                    }
                } else {
                    self.cats.insert(key, value.clone());
                    IronClusterWriteResponse {
                        applied: true,
                        value: Some(IronClusterEntity::Cat(value)),
                        previous_value: None,
                        message: None,
                    }
                }
            }
            IronClusterEntity::Dog(value) => {
                let key = value.entity_key();
                if let Some(existing_value) = self.dogs.get(&key).cloned() {
                    IronClusterWriteResponse {
                        applied: false,
                        value: Some(IronClusterEntity::Dog(existing_value.clone())),
                        previous_value: Some(IronClusterEntity::Dog(existing_value)),
                        message: Some(format!("狗数据键已存在，新增失败: {key}")),
                    }
                } else {
                    self.dogs.insert(key, value.clone());
                    IronClusterWriteResponse {
                        applied: true,
                        value: Some(IronClusterEntity::Dog(value)),
                        previous_value: None,
                        message: None,
                    }
                }
            }
        }
    }

    // 修改集群数据实体。
    fn update_entity(
        &mut self,
        value: IronClusterEntity,
    ) -> IronClusterWriteResponse<IronClusterEntity> {
        match value {
            IronClusterEntity::Cat(value) => {
                let key = value.entity_key();
                if let Some(previous_value) = self.cats.get(&key).cloned() {
                    self.cats.insert(key, value.clone());
                    IronClusterWriteResponse {
                        applied: true,
                        value: Some(IronClusterEntity::Cat(value)),
                        previous_value: Some(IronClusterEntity::Cat(previous_value)),
                        message: None,
                    }
                } else {
                    IronClusterWriteResponse {
                        applied: false,
                        value: None,
                        previous_value: None,
                        message: Some(format!("猫数据键不存在，修改失败: {key}")),
                    }
                }
            }
            IronClusterEntity::Dog(value) => {
                let key = value.entity_key();
                if let Some(previous_value) = self.dogs.get(&key).cloned() {
                    self.dogs.insert(key, value.clone());
                    IronClusterWriteResponse {
                        applied: true,
                        value: Some(IronClusterEntity::Dog(value)),
                        previous_value: Some(IronClusterEntity::Dog(previous_value)),
                        message: None,
                    }
                } else {
                    IronClusterWriteResponse {
                        applied: false,
                        value: None,
                        previous_value: None,
                        message: Some(format!("狗数据键不存在，修改失败: {key}")),
                    }
                }
            }
        }
    }

    // 删除集群数据实体。
    fn delete_entity(
        &mut self,
        value: IronClusterEntity,
    ) -> IronClusterWriteResponse<IronClusterEntity> {
        match value {
            IronClusterEntity::Cat(value) => {
                let key = value.entity_key();
                if let Some(previous_value) = self.cats.remove(&key) {
                    IronClusterWriteResponse {
                        applied: true,
                        value: None,
                        previous_value: Some(IronClusterEntity::Cat(previous_value)),
                        message: None,
                    }
                } else {
                    IronClusterWriteResponse {
                        applied: false,
                        value: None,
                        previous_value: None,
                        message: Some(format!("猫数据键不存在，删除失败: {key}")),
                    }
                }
            }
            IronClusterEntity::Dog(value) => {
                let key = value.entity_key();
                if let Some(previous_value) = self.dogs.remove(&key) {
                    IronClusterWriteResponse {
                        applied: true,
                        value: None,
                        previous_value: Some(IronClusterEntity::Dog(previous_value)),
                        message: None,
                    }
                } else {
                    IronClusterWriteResponse {
                        applied: false,
                        value: None,
                        previous_value: None,
                        message: Some(format!("狗数据键不存在，删除失败: {key}")),
                    }
                }
            }
        }
    }
}
