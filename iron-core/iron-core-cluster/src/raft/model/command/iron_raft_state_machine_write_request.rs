use crate::contract::iron_cluster_entity_model_source_node_tagged::IronClusterEntityModelSourceNodeObjectRef;

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
