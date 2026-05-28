// IronMesh 集群验证用狗数据模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IronDog {
    pub id: u64,       // 狗数据唯一标识，用作状态机键。
    pub name: String,  // 狗数据名称。
    pub color: String, // 狗数据颜色描述。
}
