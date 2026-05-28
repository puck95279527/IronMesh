use iron_core_cluster::IronCat;
use iron_core_cluster::IronClusterEntityModelSourceNodeTagged;
use iron_core_cluster::IronDog;

// 构建测试用猫数据。
fn cat(id: u32, name: &str) -> IronCat {
    IronCat {
        id,
        name: name.to_string(),
        age: format!("{name}-age"),
    }
}

// 构建测试用狗数据。
fn dog(id: u64, name: &str) -> IronDog {
    IronDog {
        id,
        name: name.to_string(),
        color: format!("{name}-color"),
    }
}

// 验证猫数据会生成稳定的来源节点对象标识。
#[test]
fn cat_source_node_object_ref_is_stable() {
    let object_ref = cat(101, "cat-a")
        .source_node_object_ref()
        .expect("cat must expose source node object ref");

    assert_eq!(object_ref.entity_type, "IronCat");
    assert_eq!(object_ref.entity_key, "101");
    assert_eq!(object_ref.index_key(), "IronCat:101");
}

// 验证猫数据可以只根据实体键生成来源节点对象标识。
#[test]
fn cat_source_node_object_ref_from_key_is_stable() {
    let object_ref = IronCat::source_node_object_ref_from_key(&101)
        .expect("cat key must expose source node object ref");

    assert_eq!(object_ref.entity_type, "IronCat");
    assert_eq!(object_ref.entity_key, "101");
    assert_eq!(object_ref.index_key(), "IronCat:101");
}

// 验证狗数据不会进入来源节点索引。
#[test]
fn dog_source_node_object_ref_is_none() {
    assert!(dog(101, "dog-a").source_node_object_ref().is_none());
}

// 验证狗数据键不会进入来源节点索引。
#[test]
fn dog_source_node_object_ref_from_key_is_none() {
    assert!(IronDog::source_node_object_ref_from_key(&101).is_none());
}
