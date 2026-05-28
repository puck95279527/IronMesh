use crate::contract::iron_cluster_entity_model::IronClusterEntityModel;
use crate::contract::iron_cluster_entity_model_source_node_tagged::IronClusterEntityModelSourceNodeObjectRef;
use crate::contract::iron_cluster_entity_model_source_node_tagged::IronClusterEntityModelSourceNodeTagged;
use crate::data_plane::iron_cluster_entity::IronClusterEntity;
use crate::raft::model::command::iron_cluster_write_request::IronClusterWriteRequest;

// IronMesh Raft 来源节点索引动作。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronRaftSourceNodeIndexAction<W> {
    // 记录对象与来源节点的绑定关系。
    Track {
        object_ref: IronClusterEntityModelSourceNodeObjectRef, // 需要记录来源节点的对象标识。
        delete_request: W,                                     // 清理来源节点时用于删除对象的请求。
    },
    // 移除对象与来源节点的绑定关系。
    Remove {
        object_ref: IronClusterEntityModelSourceNodeObjectRef, // 需要从来源节点索引中移除的对象标识。
    },
}

// IronMesh Raft 内部状态机写入请求。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronRaftStateMachineWriteRequest<W> {
    // 普通数据面写入请求。
    Data {
        source_node_id: u64, // 发起写入的来源节点 ID。
        data_request: W,     // 数据面原始写入请求。
        source_node_index_action: Option<IronRaftSourceNodeIndexAction<W>>, // 来源节点索引动作。
    },
    // 清理指定来源节点写入的数据。
    CleanSourceNodeData {
        source_node_id: u64, // 需要清理的来源节点 ID。
    },
}

impl<W> IronRaftStateMachineWriteRequest<W> {
    // 创建数据面写入请求。
    pub fn data(
        source_node_id: u64,
        data_request: W,
        source_node_index_action: Option<IronRaftSourceNodeIndexAction<W>>,
    ) -> Self {
        Self::Data {
            source_node_id,
            data_request,
            source_node_index_action,
        }
    }

    // 创建来源节点数据清理请求。
    pub fn clean_source_node_data(source_node_id: u64) -> Self {
        Self::CleanSourceNodeData { source_node_id }
    }
}

impl IronRaftStateMachineWriteRequest<IronClusterWriteRequest<IronClusterEntity>> {
    // 创建默认集群实体新增写入请求。
    pub fn cluster_insert<T>(source_node_id: u64, value: T) -> Self
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        let source_node_index_action = Self::track_cluster_source_node_index_action(&value);
        Self::data(
            source_node_id,
            IronClusterWriteRequest::insert(value),
            source_node_index_action,
        )
    }

    // 创建默认集群实体修改写入请求。
    pub fn cluster_update<T>(source_node_id: u64, value: T) -> Self
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        let source_node_index_action = Self::track_cluster_source_node_index_action(&value);
        Self::data(
            source_node_id,
            IronClusterWriteRequest::update(value),
            source_node_index_action,
        )
    }

    // 创建默认集群实体删除写入请求。
    pub fn cluster_delete<T>(source_node_id: u64, value: T) -> Self
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        let source_node_index_action = value
            .source_node_object_ref()
            .map(|object_ref| IronRaftSourceNodeIndexAction::Remove { object_ref });
        Self::data(
            source_node_id,
            IronClusterWriteRequest::delete(value),
            source_node_index_action,
        )
    }

    // 创建默认集群实体按键删除写入请求。
    pub fn cluster_delete_key<T>(source_node_id: u64, key: T::Key) -> Self
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        let source_node_index_action = T::source_node_object_ref_from_key(&key)
            .map(|object_ref| IronRaftSourceNodeIndexAction::Remove { object_ref });
        Self::data(
            source_node_id,
            IronClusterWriteRequest::delete_key::<T>(key),
            source_node_index_action,
        )
    }

    // 创建默认集群实体来源节点索引记录动作。
    fn track_cluster_source_node_index_action<T>(
        value: &T,
    ) -> Option<IronRaftSourceNodeIndexAction<IronClusterWriteRequest<IronClusterEntity>>>
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        value
            .source_node_object_ref()
            .map(|object_ref| IronRaftSourceNodeIndexAction::Track {
                object_ref,
                delete_request: IronClusterWriteRequest::delete_key::<T>(value.entity_key()),
            })
    }
}
