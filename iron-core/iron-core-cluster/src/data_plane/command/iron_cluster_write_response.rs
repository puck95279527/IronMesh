use crate::raft::model::command::iron_raft_source_node_index_write_response::IronRaftSourceNodeIndexWriteResponse;

// IronMesh 集群写入响应模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IronClusterWriteResponse<V> {
    pub applied: bool,             // 当前写入命令是否实际修改了状态机。
    pub value: Option<V>, // 操作目标当前关联的值，新增和修改成功后为新值，失败时可为现有值。
    pub previous_value: Option<V>, // 操作生效前的旧值，用于表达覆盖、删除或冲突结果。
    pub message: Option<String>, // 当前写入结果说明，成功时通常为空。
}

impl<V> Default for IronClusterWriteResponse<V> {
    // 创建空的集群写入响应。
    fn default() -> Self {
        Self {
            applied: false,
            value: None,
            previous_value: None,
            message: None,
        }
    }
}

impl<V> IronRaftSourceNodeIndexWriteResponse for IronClusterWriteResponse<V> {
    // 判断集群写入响应是否实际修改了数据。
    fn source_node_index_applied(&self) -> bool {
        self.applied
    }
}
