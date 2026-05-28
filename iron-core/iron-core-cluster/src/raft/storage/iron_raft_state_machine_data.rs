use serde::de::DeserializeOwned;

// IronMesh Raft 状态机数据范式，用于约束可被 Raft 存储层托管的数据模型。
pub trait IronRaftStateMachineData:
    Clone + Default + serde::Serialize + DeserializeOwned + Send + Sync + 'static
{
    // 状态机写入请求类型。
    type WriteRequest: Clone
        + std::fmt::Debug
        + serde::Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static;

    // 状态机写入响应类型。
    type WriteResponse: Clone
        + Default
        + std::fmt::Debug
        + serde::Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static;

    // 应用一条 Raft 写入请求，并返回写入结果。
    fn apply_raft_request(&mut self, request: Self::WriteRequest) -> Self::WriteResponse;

    // 判断写入响应是否代表状态机实际发生变更。
    fn write_response_applied(_response: &Self::WriteResponse) -> bool {
        true
    }

    // 创建来源节点数据清理请求，不支持来源节点索引的状态机默认不提供。
    fn clean_source_node_data_request(_source_node_id: u64) -> Option<Self::WriteRequest> {
        None
    }
}
