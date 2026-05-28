use crate::contract::iron_cluster_entity_model::IronClusterEntityModel;
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
    pub fn insert<T>(value: T) -> Self
    where
        T: IronClusterEntityModel + Into<IronClusterEntity>,
    {
        Self::Insert(value.into())
    }

    // 创建默认集群实体修改请求。
    pub fn update<T>(value: T) -> Self
    where
        T: IronClusterEntityModel + Into<IronClusterEntity>,
    {
        Self::Update(value.into())
    }

    // 创建默认集群实体删除请求。
    pub fn delete<T>(value: T) -> Self
    where
        T: IronClusterEntityModel + Into<IronClusterEntity>,
    {
        Self::Delete(value.into())
    }

    // 根据默认集群实体键创建删除请求。
    pub fn delete_key<T>(key: T::Key) -> Self
    where
        T: IronClusterEntityModel + Into<IronClusterEntity>,
    {
        Self::Delete(T::from_entity_key(key).into())
    }
}
