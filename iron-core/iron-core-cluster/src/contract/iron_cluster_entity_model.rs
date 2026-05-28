// IronMesh 集群实体模型约束，用于要求数据面实体提供稳定键。
pub trait IronClusterEntityModel {
    // 实体键类型。
    type Key;

    // 读取实体键。
    fn entity_key(&self) -> Self::Key;

    // 根据实体键构造实体。
    fn from_entity_key(key: Self::Key) -> Self;
}
