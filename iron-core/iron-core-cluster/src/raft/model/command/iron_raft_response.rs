// IronMesh Raft 最小响应模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronRaftResponse {
    pub value: Option<String>, // 当前请求返回的可选值。
}
