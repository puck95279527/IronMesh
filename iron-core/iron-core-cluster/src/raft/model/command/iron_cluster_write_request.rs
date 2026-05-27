use crate::data_plane::iron_cat::IronCat;

// IronMesh 集群写入请求模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronClusterWriteRequest<K = u64, V = IronCat> {
    Insert(V), // 新增指定值，键由状态机 apply 逻辑解释。
    Update(V), // 修改指定值，键由状态机 apply 逻辑解释。
    Delete(K), // 删除指定键对应的值。
}
