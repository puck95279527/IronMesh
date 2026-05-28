use crate::contract::iron_cluster_entity_model::IronClusterEntityModel;
use crate::data_plane::iron_cluster_entity::IronClusterEntity;

// IronMesh 集群验证用狗数据模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct IronDog {
    pub id: u64,       // 狗数据唯一标识，用作状态机键。
    pub name: String,  // 狗数据名称。
    pub color: String, // 狗数据颜色描述。
}

impl IronClusterEntityModel for IronDog {
    type Key = u64;

    // 读取狗数据键。
    fn entity_key(&self) -> Self::Key {
        self.id
    }

    // 根据狗数据键构造狗数据。
    fn from_entity_key(key: Self::Key) -> Self {
        Self {
            id: key,
            name: String::new(),
            color: String::new(),
        }
    }
}

impl From<IronDog> for IronClusterEntity {
    // 将狗数据转换为集群实体。
    fn from(value: IronDog) -> Self {
        Self::Dog(value)
    }
}
