use crate::data_plane::model::iron_cat::IronCat;
use crate::data_plane::model::iron_dog::IronDog;

// IronMesh 集群验证用可传输实体。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronClusterEntity {
    Cat(IronCat), // 猫数据实体。
    Dog(IronDog), // 狗数据实体。
}
