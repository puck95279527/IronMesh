// IronMesh Raft 来源节点索引写入响应判断契约。
pub trait IronRaftSourceNodeIndexWriteResponse {
    // 判断写入响应是否代表状态机实际发生变更。
    fn source_node_index_applied(&self) -> bool;
}
