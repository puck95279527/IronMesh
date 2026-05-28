use std::collections::BTreeMap;

use crate::contract::iron_cluster_entity_model_source_node_tagged::IronClusterEntityModelSourceNodeObjectRef;
use crate::data_plane::iron_cluster_entity::IronClusterEntity;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::model::command::iron_raft_state_machine_write_request::IronRaftSourceNodeIndexAction;
use crate::raft::model::command::iron_raft_state_machine_write_request::IronRaftStateMachineWriteRequest;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh Raft 容器索引写入响应判断。
pub trait IronRaftSourceNodeIndexWriteResponse {
    // 判断写入响应是否代表状态机实际发生变更。
    fn source_node_index_applied(&self) -> bool;
}

impl IronRaftSourceNodeIndexWriteResponse for IronClusterWriteResponse<IronClusterEntity> {
    // 判断默认集群写入响应是否实际修改了数据。
    fn source_node_index_applied(&self) -> bool {
        self.applied
    }
}

// IronMesh Raft 来源节点索引记录。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IronRaftSourceNodeIndexRecord<W> {
    pub object_ref: IronClusterEntityModelSourceNodeObjectRef, // 被来源节点索引记录的对象标识。
    pub delete_request: W,                                     // 清理来源节点时用于删除对象的请求。
}

// IronMesh Raft 状态机总容器。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(bound(
    serialize = "D: serde::Serialize, D::WriteRequest: serde::Serialize",
    deserialize = "D: serde::de::DeserializeOwned, D::WriteRequest: serde::de::DeserializeOwned"
))]
pub struct IronRaftStateMachineContainer<D>
where
    D: IronRaftStateMachineData,
{
    pub cluster_state: D, // 集群数据面状态。
    pub source_node_index:
        BTreeMap<u64, BTreeMap<String, IronRaftSourceNodeIndexRecord<D::WriteRequest>>>, // 来源节点数据索引。
}

impl<D> Default for IronRaftStateMachineContainer<D>
where
    D: IronRaftStateMachineData,
{
    // 创建默认 Raft 状态机总容器。
    fn default() -> Self {
        Self {
            cluster_state: D::default(),
            source_node_index: BTreeMap::new(),
        }
    }
}

impl<D> IronRaftStateMachineContainer<D>
where
    D: IronRaftStateMachineData,
{
    // 根据索引动作维护来源节点索引。
    fn apply_source_node_index_action(
        &mut self,
        source_node_id: u64,
        action: IronRaftSourceNodeIndexAction<D::WriteRequest>,
    ) {
        match action {
            IronRaftSourceNodeIndexAction::Track {
                object_ref,
                delete_request,
            } => {
                let index_key = object_ref.index_key();
                self.remove_source_node_object(&object_ref);
                self.source_node_index
                    .entry(source_node_id)
                    .or_default()
                    .insert(
                        index_key,
                        IronRaftSourceNodeIndexRecord {
                            object_ref,
                            delete_request,
                        },
                    );
            }
            IronRaftSourceNodeIndexAction::Remove { object_ref } => {
                self.remove_source_node_object(&object_ref);
            }
        }
    }

    // 从全部来源节点索引中移除指定对象。
    fn remove_source_node_object(
        &mut self,
        object_ref: &IronClusterEntityModelSourceNodeObjectRef,
    ) {
        let index_key = object_ref.index_key();
        self.source_node_index.retain(|_, objects| {
            objects.remove(&index_key);
            !objects.is_empty()
        });
    }

    // 清理指定来源节点写入的所有已索引数据。
    fn clean_source_node_data(&mut self, source_node_id: u64) -> D::WriteResponse {
        let Some(objects) = self.source_node_index.remove(&source_node_id) else {
            tracing::info!(
                source_node_id,
                affected_count = 0usize,
                "[Iron] [cluster-data] 来源节点没有可清理数据"
            );
            return D::WriteResponse::default();
        };

        let mut affected_count = 0usize;
        for (_, record) in objects {
            let response = self.cluster_state.apply_raft_request(record.delete_request);
            if response.source_node_index_applied() {
                affected_count += 1;
                self.remove_source_node_object(&record.object_ref);
            }
        }

        tracing::info!(
            source_node_id,
            affected_count,
            "[Iron] [cluster-data] 来源节点数据清理完成"
        );
        D::WriteResponse::default()
    }
}

impl<D> IronRaftStateMachineData for IronRaftStateMachineContainer<D>
where
    D: IronRaftStateMachineData,
    D::WriteResponse: IronRaftSourceNodeIndexWriteResponse,
{
    type WriteRequest = IronRaftStateMachineWriteRequest<D::WriteRequest>;
    type WriteResponse = D::WriteResponse;

    // 应用一条 Raft 写入请求到状态机总容器。
    fn apply_raft_request(&mut self, request: Self::WriteRequest) -> Self::WriteResponse {
        match request {
            IronRaftStateMachineWriteRequest::Data {
                source_node_id,
                data_request,
                source_node_index_action,
            } => {
                let response = self.cluster_state.apply_raft_request(data_request);
                if response.source_node_index_applied() {
                    if let Some(action) = source_node_index_action {
                        self.apply_source_node_index_action(source_node_id, action);
                    }
                }
                response
            }
            IronRaftStateMachineWriteRequest::CleanSourceNodeData { source_node_id } => {
                self.clean_source_node_data(source_node_id)
            }
        }
    }

    // 判断容器写入响应是否代表状态机实际发生变更。
    // 创建默认来源节点数据清理请求。
    fn clean_source_node_data_request(source_node_id: u64) -> Option<Self::WriteRequest> {
        Some(IronRaftStateMachineWriteRequest::clean_source_node_data(
            source_node_id,
        ))
    }
}
