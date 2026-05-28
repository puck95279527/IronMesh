use crate::data_plane::iron_cluster_entity::IronClusterEntity;
use crate::data_plane::model::iron_cat::IronCat;
use crate::data_plane::model::iron_dog::IronDog;

// IronMesh 集群验证用可传输实体值。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronClusterEntityValue {
    Cat(IronCat), // 猫数据实体值。
    Dog(IronDog), // 狗数据实体值。
}

impl IronClusterEntity for IronClusterEntityValue {
    type Key = u64;

    // 读取集群实体值键。
    fn entity_key(&self) -> Self::Key {
        match self {
            Self::Cat(value) => value.entity_key(),
            Self::Dog(value) => value.entity_key(),
        }
    }
}
