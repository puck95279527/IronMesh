// 集群控制面 HTTP 接口。

use crate::model::IronClusterCommand;
use crate::model::IronClusterHttpState;
use crate::model::IronClusterServiceRecord;
use crate::model::IronRaft;
use crate::model::IronRaftStore;
use crate::model::IronRaftTypeConfig;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::raft::AppendEntriesRequest;
use openraft::raft::VoteRequest;

// 集群内部共享密钥 HTTP 头。
pub(crate) const IRON_CLUSTER_TOKEN_HEADER: &str = "x-iron-cluster-token";

// 构建集群控制面 HTTP 路由。
pub(crate) fn build_cluster_http_router(
    cluster_token: String,
    raft: IronRaft,
    store: IronRaftStore,
) -> Router {
    let state = IronClusterHttpState {
        cluster_token,
        raft,
        store,
    };

    Router::new()
        .route("/iron/cluster/health", get(health_http_handler))
        .route("/iron/cluster/services", get(services_http_handler))
        .route("/iron/cluster/register", post(register_http_handler))
        .route("/iron/cluster/raft/append", post(raft_append_http_handler))
        .route("/iron/cluster/raft/vote", post(raft_vote_http_handler))
        .with_state(state)
}

// 集群服务注册 HTTP 处理函数。
async fn register_http_handler(
    State(state): State<IronClusterHttpState>,
    headers: HeaderMap,
    Json(record): Json<IronClusterServiceRecord>,
) -> impl IntoResponse {
    if !is_valid_cluster_token(&headers, &state.cluster_token) {
        return (StatusCode::UNAUTHORIZED, "集群密钥无效").into_response();
    }

    match state
        .raft
        .client_write(IronClusterCommand::RegisterService(record))
        .await
    {
        Ok(_) => Json(state.store.registry_snapshot().await).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("集群 Raft 写入失败: {error}"),
        )
            .into_response(),
    }
}

// 集群服务发现 HTTP 处理函数。
async fn services_http_handler(
    State(state): State<IronClusterHttpState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !is_valid_cluster_token(&headers, &state.cluster_token) {
        return (StatusCode::UNAUTHORIZED, "集群密钥无效").into_response();
    }

    Json(state.store.registry_snapshot().await).into_response()
}

// 集群健康检查 HTTP 处理函数。
async fn health_http_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

// Raft AppendEntries HTTP 处理函数。
async fn raft_append_http_handler(
    State(state): State<IronClusterHttpState>,
    headers: HeaderMap,
    Json(request): Json<AppendEntriesRequest<IronRaftTypeConfig>>,
) -> impl IntoResponse {
    if !is_valid_cluster_token(&headers, &state.cluster_token) {
        return (StatusCode::UNAUTHORIZED, "集群密钥无效").into_response();
    }

    match state.raft.append_entries(request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("集群 Raft append 失败: {error}"),
        )
            .into_response(),
    }
}

// Raft RequestVote HTTP 处理函数。
async fn raft_vote_http_handler(
    State(state): State<IronClusterHttpState>,
    headers: HeaderMap,
    Json(request): Json<VoteRequest<u64>>,
) -> impl IntoResponse {
    if !is_valid_cluster_token(&headers, &state.cluster_token) {
        return (StatusCode::UNAUTHORIZED, "集群密钥无效").into_response();
    }

    match state.raft.vote(request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("集群 Raft vote 失败: {error}"),
        )
            .into_response(),
    }
}

// 判断请求是否携带正确的集群密钥。
fn is_valid_cluster_token(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(IRON_CLUSTER_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|actual| actual == expected)
}
