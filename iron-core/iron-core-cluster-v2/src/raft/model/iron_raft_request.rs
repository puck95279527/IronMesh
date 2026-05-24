// IronMesh Raft 最小请求模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronRaftRequest {
    // 设置指定键的字符串值。
    Set {
        key: String, // 需要写入的键。
        value: String, // 需要写入的值。
    },
}
