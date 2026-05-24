// 注册中心验证 HTTP 接口。

use crate::model::ClusterDebugHttpState;
use crate::model::ClusterRegistryRuntimeNode;
use crate::model::IronClusterService;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use std::collections::BTreeMap;

// 构建注册中心验证 HTTP 路由。
pub(crate) fn build_registry_debug_http_router(nodes: Vec<ClusterRegistryRuntimeNode>) -> Router {
    Router::new()
        .route("/iron/cluster/health", get(health_http_handler))
        .route("/iron/cluster/services", get(services_http_handler))
        .with_state(ClusterDebugHttpState { nodes })
}

// 注册中心健康检查 HTTP 处理函数。
async fn health_http_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

// 注册中心服务发现 HTTP 处理函数。
async fn services_http_handler(State(state): State<ClusterDebugHttpState>) -> impl IntoResponse {
    let mut best_snapshot = None;

    for node in &state.nodes {
        let snapshot = node.store.registry_snapshot().await;
        if best_snapshot
            .as_ref()
            .is_none_or(|current: &BTreeMap<String, IronClusterService>| {
                snapshot.len() >= current.len()
            })
        {
            best_snapshot = Some(snapshot);
        }
    }

    let snapshot = best_snapshot.unwrap_or_default();
    Json(snapshot)
}
