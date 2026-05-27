// IronMesh 集群写入响应模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronClusterWriteResponse {
    pub value: Option<String>, // 当前请求返回的可选值。
}
