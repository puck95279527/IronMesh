// IronMesh 集群写入响应模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronClusterWriteResponse<V> {
    pub applied: bool,             // 当前写入命令是否实际修改了状态机。
    pub value: Option<V>, // 操作目标当前关联的值，新增和修改成功后为新值，失败时可为现有值。
    pub previous_value: Option<V>, // 操作生效前的旧值，用于表达覆盖、删除或冲突结果。
    pub message: Option<String>, // 当前写入结果说明，成功时通常为空。
}
