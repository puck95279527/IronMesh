// 注册中心验证 HTTP 接口。

use crate::model::IronClusterRegistryRuntimeNode;
use crate::model::IronClusterState;
use crate::model::IronRegistryDebugHttpState;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;

// 构建注册中心验证 HTTP 路由。
pub(crate) fn build_registry_debug_http_router(
    nodes: Vec<IronClusterRegistryRuntimeNode>,
) -> Router {
    Router::new()
        .route("/iron/cluster/health", get(health_http_handler))
        .route("/iron/cluster/services", get(services_http_handler))
        .with_state(IronRegistryDebugHttpState { nodes })
}

// 注册中心健康检查 HTTP 处理函数。
async fn health_http_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

// 注册中心服务发现 HTTP 处理函数。
async fn services_http_handler(
    State(state): State<IronRegistryDebugHttpState>,
) -> impl IntoResponse {
    let mut best_snapshot = None;

    for node in &state.nodes {
        let snapshot = node.store.registry_snapshot().await;
        if best_snapshot
            .as_ref()
            .is_none_or(|current: &crate::model::IronClusterRegistry| {
                snapshot.metadata_version >= current.metadata_version
            })
        {
            best_snapshot = Some(snapshot);
        }
    }

    let mut snapshot = best_snapshot.unwrap_or_default();
    for record in snapshot.services.values_mut() {
        if record.endpoints.is_empty() {
            record.state = IronClusterState::Offline;
        }
    }

    Json(snapshot)
}
