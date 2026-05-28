use crate::data_plane::model::iron_cat::IronCat;
use crate::data_plane::model::iron_dog::IronDog;

// IronMesh 集群写入请求模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronClusterWriteRequest {
    InsertCat(IronCat), // 新增猫数据。
    UpdateCat(IronCat), // 修改猫数据。
    DeleteCat(u64),     // 删除指定键对应的猫数据。
    InsertDog(IronDog), // 新增狗数据。
    UpdateDog(IronDog), // 修改狗数据。
    DeleteDog(u64),     // 删除指定键对应的狗数据。
}
