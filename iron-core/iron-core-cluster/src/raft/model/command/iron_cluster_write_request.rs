// IronMesh 集群写入请求模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronClusterWriteRequest {
    // 新增指定键的字符串值，键已存在时不会覆盖旧值。
    Insert {
        key: String,   // 需要新增的键。
        value: String, // 需要新增的值。
    },
    // 修改指定键的字符串值，键不存在时不会创建新值。
    Update {
        key: String,   // 需要修改的键。
        value: String, // 需要修改的新值。
    },
    // 删除指定键的字符串值，键不存在时不会修改状态机。
    Delete {
        key: String, // 需要删除的键。
    },
}
