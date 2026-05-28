use crate::data_plane::model::iron_cat::IronCat;
use crate::data_plane::model::iron_dog::IronDog;

// IronMesh 集群实体模型约束，用于要求数据面实体能提供键并能进入集群实体总类型。
pub trait IronClusterEntityModel: Into<IronClusterEntity> {
    // 实体键类型。
    type Key;

    // 读取实体键。
    fn entity_key(&self) -> Self::Key;
}

// IronMesh 集群验证用可传输实体。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronClusterEntity {
    Cat(IronCat), // 猫数据实体。
    Dog(IronDog), // 狗数据实体。
}
