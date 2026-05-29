use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Cursor;

use iron_core_cluster::raft::IronTypeConfig;
use iron_core_cluster::raft::network::IronTcpRequest;
use iron_core_cluster::raft::network::IronTcpResponse;
use iron_core_cluster::raft::storage::IronStateMachine;
use openraft::BasicNode;
use openraft::CommittedLeaderId;
use openraft::LogId;
use openraft::Membership;
use openraft::RaftSnapshotBuilder;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use openraft::Vote;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::storage::RaftStateMachine;

// 安装快照后状态机应该恢复快照携带的日志进度和成员关系。
#[tokio::test]
async fn install_snapshot_updates_state_machine_progress() {
    let mut state_machine = IronStateMachine::default();
    let log_id = LogId::new(CommittedLeaderId::new(3, 1), 8);
    let membership = Membership::new(
        vec![BTreeSet::from([1, 2])],
        BTreeMap::from([
            (1, BasicNode::new("127.0.0.1:5001")),
            (2, BasicNode::new("127.0.0.1:5002")),
        ]),
    );
    let stored_membership = StoredMembership::new(Some(log_id), membership);
    let snapshot_meta = SnapshotMeta {
        last_log_id: Some(log_id),
        last_membership: stored_membership.clone(),
        snapshot_id: "snapshot-test-8".to_string(),
    };

    state_machine
        .install_snapshot(&snapshot_meta, Box::new(Cursor::new(Vec::new())))
        .await
        .expect("安装快照应该成功");

    let (applied_log_id, applied_membership) = state_machine
        .applied_state()
        .await
        .expect("读取状态机进度应该成功");
    assert_eq!(Some(log_id), applied_log_id);
    assert_eq!(stored_membership, applied_membership);
}

// 构建快照后当前快照应该具备可追踪的非空快照标识。
#[tokio::test]
async fn build_snapshot_keeps_current_snapshot_meta() {
    let mut state_machine = IronStateMachine::default();

    let snapshot = state_machine
        .build_snapshot()
        .await
        .expect("构建快照应该成功");
    assert!(!snapshot.meta.snapshot_id.is_empty());

    let current_snapshot = state_machine
        .get_current_snapshot()
        .await
        .expect("读取当前快照应该成功")
        .expect("当前快照应该存在");
    assert_eq!(snapshot.meta, current_snapshot.meta);
}

// 安装快照分片请求和响应应该能通过当前 TCP JSON 协议往返。
#[test]
fn install_snapshot_message_roundtrips_through_json() {
    let log_id = LogId::new(CommittedLeaderId::new(4, 1), 16);
    let membership = Membership::new(
        vec![BTreeSet::from([1])],
        BTreeMap::from([(1, BasicNode::new("127.0.0.1:5001"))]),
    );
    let snapshot_meta = SnapshotMeta {
        last_log_id: Some(log_id),
        last_membership: StoredMembership::new(Some(log_id), membership),
        snapshot_id: "snapshot-test-16".to_string(),
    };
    let request = IronTcpRequest::InstallSnapshot(InstallSnapshotRequest::<IronTypeConfig> {
        vote: Vote::new_committed(4, 1),
        meta: snapshot_meta,
        offset: 0,
        data: vec![1, 2, 3],
        done: true,
    });

    let encoded_request = serde_json::to_vec(&request).expect("安装快照请求应该可以编码");
    let decoded_request: IronTcpRequest =
        serde_json::from_slice(&encoded_request).expect("安装快照请求应该可以解码");
    assert!(matches!(
        decoded_request,
        IronTcpRequest::InstallSnapshot(_)
    ));

    let response = IronTcpResponse::InstallSnapshot(Ok(InstallSnapshotResponse {
        vote: Vote::new(4, 1),
    }));
    let encoded_response = serde_json::to_vec(&response).expect("安装快照响应应该可以编码");
    let decoded_response: IronTcpResponse =
        serde_json::from_slice(&encoded_response).expect("安装快照响应应该可以解码");
    assert!(matches!(
        decoded_response,
        IronTcpResponse::InstallSnapshot(Ok(_))
    ));
}
