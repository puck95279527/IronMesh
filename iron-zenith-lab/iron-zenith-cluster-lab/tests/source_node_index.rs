use iron_core_cluster::{
    IronCat, IronClusterEntity, IronClusterEntityModelSourceNodeTagged, IronClusterState,
    IronClusterWriteRequest, IronDog, IronRaftSourceNodeIndexAction, IronRaftStateMachineContainer,
    IronRaftStateMachineData, IronRaftStateMachineWriteRequest,
};

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

// 构建猫数据来源节点索引记录动作。
fn cat_track_action(
    id: u32,
) -> IronRaftSourceNodeIndexAction<IronClusterWriteRequest<IronClusterEntity>> {
    IronRaftSourceNodeIndexAction::Track {
        object_ref: IronCat::source_node_object_ref_from_key(&id).expect("cat must be indexed"),
        delete_request: IronClusterWriteRequest::delete_key::<IronCat>(id),
    }
}

// 构建猫数据来源节点索引移除动作。
fn cat_remove_action(
    id: u32,
) -> IronRaftSourceNodeIndexAction<IronClusterWriteRequest<IronClusterEntity>> {
    IronRaftSourceNodeIndexAction::Remove {
        object_ref: IronCat::source_node_object_ref_from_key(&id).expect("cat must be indexed"),
    }
}

// 应用一条带来源节点信息的数据写入请求。
fn apply_data(
    state: &mut IronRaftStateMachineContainer<IronClusterState>,
    source_node_id: u64,
    data_request: IronClusterWriteRequest<IronClusterEntity>,
    action: Option<IronRaftSourceNodeIndexAction<IronClusterWriteRequest<IronClusterEntity>>>,
) {
    state.apply_raft_request(IronRaftStateMachineWriteRequest::data(
        source_node_id,
        data_request,
        action,
    ));
}

// 验证猫新增会记录来源节点索引。
#[test]
fn cat_insert_records_source_node_index() {
    let mut state = IronRaftStateMachineContainer::<IronClusterState>::default();
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::insert(cat(101, "cat-a")),
        Some(cat_track_action(101)),
    );

    let object_ref = IronCat::source_node_object_ref_from_key(&101).expect("cat must be indexed");
    assert!(state.cluster_state.cats.contains_key(&101));
    assert!(
        state
            .source_node_index
            .get(&5)
            .is_some_and(|objects| objects.contains_key(&object_ref.index_key()))
    );
}

// 验证狗新增不会记录来源节点索引。
#[test]
fn dog_insert_does_not_record_source_node_index() {
    let mut state = IronRaftStateMachineContainer::<IronClusterState>::default();
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::insert(dog(101, "dog-a")),
        None,
    );

    assert!(state.cluster_state.dogs.contains_key(&101));
    assert!(state.source_node_index.is_empty());
}

// 验证猫修改会迁移来源节点索引。
#[test]
fn cat_update_moves_source_node_index() {
    let mut state = IronRaftStateMachineContainer::<IronClusterState>::default();
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::insert(cat(101, "cat-a")),
        Some(cat_track_action(101)),
    );
    apply_data(
        &mut state,
        6,
        IronClusterWriteRequest::update(cat(101, "cat-b")),
        Some(cat_track_action(101)),
    );

    let object_ref = IronCat::source_node_object_ref_from_key(&101).expect("cat must be indexed");
    assert!(!state.source_node_index.contains_key(&5));
    assert!(
        state
            .source_node_index
            .get(&6)
            .is_some_and(|objects| objects.contains_key(&object_ref.index_key()))
    );
}

// 验证猫删除会移除来源节点索引。
#[test]
fn cat_delete_removes_source_node_index() {
    let mut state = IronRaftStateMachineContainer::<IronClusterState>::default();
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::insert(cat(101, "cat-a")),
        Some(cat_track_action(101)),
    );
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::delete_key::<IronCat>(101),
        Some(cat_remove_action(101)),
    );

    assert!(!state.cluster_state.cats.contains_key(&101));
    assert!(state.source_node_index.is_empty());
}

// 验证失败的猫新增不会污染来源节点索引。
#[test]
fn failed_cat_insert_does_not_change_source_node_index() {
    let mut state = IronRaftStateMachineContainer::<IronClusterState>::default();
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::insert(cat(101, "cat-a")),
        Some(cat_track_action(101)),
    );
    apply_data(
        &mut state,
        6,
        IronClusterWriteRequest::insert(cat(101, "cat-b")),
        Some(cat_track_action(101)),
    );

    assert!(state.source_node_index.contains_key(&5));
    assert!(!state.source_node_index.contains_key(&6));
}

// 验证来源节点清理只删除目标节点写入的猫数据。
#[test]
fn clean_source_node_data_deletes_only_target_source_cats() {
    let mut state = IronRaftStateMachineContainer::<IronClusterState>::default();
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::insert(cat(101, "cat-a")),
        Some(cat_track_action(101)),
    );
    apply_data(
        &mut state,
        6,
        IronClusterWriteRequest::insert(cat(102, "cat-b")),
        Some(cat_track_action(102)),
    );
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::insert(dog(101, "dog-a")),
        None,
    );

    state.apply_raft_request(IronRaftStateMachineWriteRequest::clean_source_node_data(5));

    assert!(!state.cluster_state.cats.contains_key(&101));
    assert!(state.cluster_state.cats.contains_key(&102));
    assert!(state.cluster_state.dogs.contains_key(&101));
    assert!(!state.source_node_index.contains_key(&5));
    assert!(state.source_node_index.contains_key(&6));
}

// 验证清理不存在的来源节点时状态保持不变。
#[test]
fn clean_missing_source_node_keeps_state_unchanged() {
    let mut state = IronRaftStateMachineContainer::<IronClusterState>::default();
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::insert(cat(101, "cat-a")),
        Some(cat_track_action(101)),
    );

    state.apply_raft_request(IronRaftStateMachineWriteRequest::clean_source_node_data(99));

    assert!(state.cluster_state.cats.contains_key(&101));
    assert!(state.source_node_index.contains_key(&5));
    assert!(!state.source_node_index.contains_key(&99));
}

// 验证来源节点索引可以作为 /raft/data 响应序列化为 JSON。
#[test]
fn source_node_index_serializes_to_json() {
    let mut state = IronRaftStateMachineContainer::<IronClusterState>::default();
    apply_data(
        &mut state,
        5,
        IronClusterWriteRequest::insert(cat(101, "cat-a")),
        Some(cat_track_action(101)),
    );

    let json = serde_json::to_string(&state).expect("container must serialize to json");

    assert!(json.contains("IronCat:101"));
    assert!(json.contains("delete_request"));
}
