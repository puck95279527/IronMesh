// IronMesh 集群实体约束，用于要求数据面实体提供稳定键。
pub trait IronClusterEntity {
    // 实体键类型。
    type Key;

    // 读取实体键。
    fn entity_key(&self) -> Self::Key;
}
