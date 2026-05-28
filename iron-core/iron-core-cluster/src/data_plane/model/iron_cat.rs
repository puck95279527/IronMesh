use crate::contract::iron_cluster_entity_model::IronClusterEntityModel;
use crate::contract::iron_cluster_entity_model_source_node_tagged::IronClusterEntityModelSourceNodeObjectRef;
use crate::contract::iron_cluster_entity_model_source_node_tagged::IronClusterEntityModelSourceNodeTagged;
use crate::data_plane::iron_cluster_entity::IronClusterEntity;

// IronMesh 集群验证用猫数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronCat {
    pub id: u32,      // 猫数据唯一标识，用作状态机键。
    pub name: String, // 猫数据名称。
    pub age: String,  // 猫数据年龄描述。
}

impl IronClusterEntityModel for IronCat {
    type Key = u32;

    // 读取猫数据键。
    fn entity_key(&self) -> Self::Key {
        self.id
    }

    // 根据猫数据键构造猫数据。
    fn from_entity_key(key: Self::Key) -> Self {
        Self {
            id: key,
            ..Self::default()
        }
    }
}

impl IronClusterEntityModelSourceNodeTagged for IronCat {
    // 根据猫数据值读取来源节点索引对象标识。
    fn source_node_object_ref(&self) -> Option<IronClusterEntityModelSourceNodeObjectRef> {
        Self::source_node_object_ref_from_key(&self.id)
    }

    // 根据猫数据键构建来源节点索引对象标识。
    fn source_node_object_ref_from_key(
        key: &Self::Key,
    ) -> Option<IronClusterEntityModelSourceNodeObjectRef> {
        Some(IronClusterEntityModelSourceNodeObjectRef::new(
            "IronCat",
            key.to_string(),
        ))
    }
}

impl From<IronCat> for IronClusterEntity {
    // 将猫数据转换为集群实体。
    fn from(value: IronCat) -> Self {
        Self::Cat(value)
    }
}
