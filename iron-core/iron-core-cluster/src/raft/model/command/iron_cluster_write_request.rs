use crate::data_plane::iron_cluster_entity::IronClusterEntity;

// IronMesh 集群写入请求模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronClusterWriteRequest<V> {
    Insert(V), // 新增指定实体值。
    Update(V), // 修改指定实体值。
    Delete(V), // 删除指定实体值，状态机只使用实体键。
}

impl IronClusterWriteRequest<IronClusterEntity> {
    // 创建默认集群实体新增请求。
    pub fn insert(value: impl Into<IronClusterEntity>) -> Self {
        Self::Insert(value.into())
    }

    // 创建默认集群实体修改请求。
    pub fn update(value: impl Into<IronClusterEntity>) -> Self {
        Self::Update(value.into())
    }

    // 创建默认集群实体删除请求。
    pub fn delete(value: impl Into<IronClusterEntity>) -> Self {
        Self::Delete(value.into())
    }
}
