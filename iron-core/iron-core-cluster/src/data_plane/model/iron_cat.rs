use crate::contract::iron_cluster_entity_model::IronClusterEntityModel;
use crate::data_plane::iron_cluster_entity::IronClusterEntity;

// IronMesh 集群验证用猫数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronCat {
    pub id: u32,      // 猫数据唯一标识，用作状态机键。
    pub name: String, // 猫数据名称。
    pub age: String,  // 猫数据年龄描述。
}

impl IronClusterEntityModel for IronCat {
    type Key = u32;

    // 读取猫数据键。
    fn entity_key(&self) -> Self::Key {
        self.id
    }
}

impl From<IronCat> for IronClusterEntity {
    // 将猫数据转换为集群实体。
    fn from(value: IronCat) -> Self {
        Self::Cat(value)
    }
}
