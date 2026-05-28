// IronMesh 来源节点索引对象标识。
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, serde::Deserialize, serde::Serialize)]
pub struct IronClusterEntityModelSourceNodeObjectRef {
    pub entity_type: String, // 来源节点索引中的实体类型名。
    pub entity_key: String,  // 来源节点索引中的实体键。
}

impl IronClusterEntityModelSourceNodeObjectRef {
    // 创建来源节点索引对象标识。
    pub fn new(entity_type: impl Into<String>, entity_key: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            entity_key: entity_key.into(),
        }
    }

    // 创建可作为 JSON map key 的稳定索引键。
    pub fn index_key(&self) -> String {
        format!("{}:{}", self.entity_type, self.entity_key)
    }
}

// IronMesh 支持来源节点标记的集群实体模型约束。
pub trait IronClusterEntityModelSourceNodeTagged:
    crate::contract::iron_cluster_entity_model::IronClusterEntityModel
{
    // 根据实体值读取来源节点索引对象标识。
    fn source_node_object_ref(&self) -> Option<IronClusterEntityModelSourceNodeObjectRef>;

    // 根据实体键构建来源节点索引对象标识。
    fn source_node_object_ref_from_key(
        key: &Self::Key,
    ) -> Option<IronClusterEntityModelSourceNodeObjectRef>;
}
