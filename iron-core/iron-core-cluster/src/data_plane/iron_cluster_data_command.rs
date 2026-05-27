// IronMesh 集群数据写命令模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronClusterDataCommand {
    // 设置指定键的字符串值，用于覆盖新增和修改两种最小写入流程。
    Set {
        key: String,   // 需要写入的键。
        value: String, // 需要写入的值。
    },
}
