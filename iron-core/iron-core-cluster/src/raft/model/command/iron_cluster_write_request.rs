// IronMesh 集群写入请求模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronClusterWriteRequest<V> {
    Insert(V), // 新增指定实体值。
    Update(V), // 修改指定实体值。
    Delete(V), // 删除指定实体值，状态机只使用实体键。
}
