use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Cursor;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::post;
use openraft::CommittedLeaderId;
use openraft::Membership;
use openraft::Raft;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use openraft::Vote;
use openraft::error::Fatal;
use openraft::error::RaftError;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

use crate::raft::model::iron_raft_full_snapshot_request::IronRaftFullSnapshotRequest;
use crate::raft::model::iron_raft_full_snapshot_response::IronRaftFullSnapshotResponse;
use crate::raft::model::iron_raft_full_snapshot_meta::IronRaftFullSnapshotMeta;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// IronMesh Raft HTTP 服务端。
pub struct IronRaftHttpServer;

impl IronRaftHttpServer {
    // 构建 Raft HTTP 路由。
    pub fn router(raft: Raft<IronRaftTypeConfig>) -> Router {
        Router::new()
            .route("/raft/append", post(Self::append_handler))
            .route("/raft/vote", post(Self::vote_handler))
            .route("/raft/full-snapshot", post(Self::full_snapshot_handler))
            .with_state(raft)
    }

    // 处理追加日志请求。
    async fn append_handler(
        State(raft): State<Raft<IronRaftTypeConfig>>,
        Json(request): Json<AppendEntriesRequest<IronRaftTypeConfig>>,
    ) -> Json<Result<AppendEntriesResponse<u64>, RaftError<u64>>> {
        Json(raft.append_entries(request).await)
    }

    // 处理投票请求。
    async fn vote_handler(
        State(raft): State<Raft<IronRaftTypeConfig>>,
        Json(request): Json<VoteRequest<u64>>,
    ) -> Json<Result<VoteResponse<u64>, RaftError<u64>>> {
        Json(raft.vote(request).await)
    }

    // 从请求构建投票状态。
    fn build_vote(request: &IronRaftFullSnapshotRequest) -> Vote<u64> {
        if request.vote_committed {
            Vote::new_committed(request.vote_term, request.vote_node_id)
        } else {
            Vote::new(request.vote_term, request.vote_node_id)
        }
    }

    // 从快照元信息传输模型恢复 SnapshotMeta。
    fn build_snapshot_meta(meta: &IronRaftFullSnapshotMeta) -> SnapshotMeta<u64, openraft::BasicNode> {
        let last_log_id = match (meta.last_log_term, meta.last_log_node_id, meta.last_log_index) {
            (Some(term), Some(node_id), Some(index)) => Some(openraft::LogId::new(CommittedLeaderId::new(term, node_id), index)),
            _ => None,
        };

        let voters = meta.membership.iter().cloned().collect::<BTreeSet<_>>();
        let nodes = meta
            .membership
            .iter()
            .map(|node_id| (*node_id, openraft::BasicNode::new(format!("127.0.0.1:500{node_id}"))))
            .collect::<BTreeMap<_, _>>();
        let membership = Membership::new(vec![voters], nodes);
        let stored_membership = StoredMembership::new(last_log_id.clone(), membership);

        SnapshotMeta {
            last_log_id,
            last_membership: stored_membership,
            snapshot_id: meta.snapshot_id.clone(),
        }
    }

    // 从投票状态构建完整快照响应。
    fn build_response(vote: Vote<u64>) -> IronRaftFullSnapshotResponse {
        IronRaftFullSnapshotResponse {
            vote_term: vote.leader_id.term,
            vote_node_id: vote.leader_id.node_id,
            vote_committed: vote.committed,
        }
    }

    // 处理完整快照请求并安装快照。
    async fn full_snapshot_handler(
        State(raft): State<Raft<IronRaftTypeConfig>>,
        Json(request): Json<IronRaftFullSnapshotRequest>,
    ) -> Json<Result<IronRaftFullSnapshotResponse, Fatal<u64>>> {
        let vote = Self::build_vote(&request);
        let meta = Self::build_snapshot_meta(&request.meta);
        let snapshot = Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(request.snapshot)),
        };

        let result = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map(|resp| Self::build_response(resp.vote));

        Json(result)
    }
}
