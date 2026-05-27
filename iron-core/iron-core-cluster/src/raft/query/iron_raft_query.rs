use std::net::SocketAddr;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use openraft::Raft;
use openraft::RaftMetrics;

use crate::raft::iron_raft_log_tag::self_tag as self_node_tag;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// 启动 Raft 查询 HTTP 服务。
#[allow(dead_code)]
pub async fn start_query_http(
    node_id: u64,
    query_port: u16,
    raft: Raft<IronRaftTypeConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if query_port == 0 {
        return Ok(());
    }

    let query_addr = format!("127.0.0.1:{query_port}");
    start_query_http_with_addr(node_id, query_addr, raft).await
}

// 启动 Raft 查询 HTTP 服务。
pub async fn start_query_http_with_addr(
    node_id: u64,
    query_addr: String,
    raft: Raft<IronRaftTypeConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if query_addr.is_empty() {
        return Ok(());
    }

    let router = Router::new()
        .route("/health", get(health_handler))
        .route("/raft/metrics", get(metrics_handler))
        .with_state(raft);

    let addr = query_addr.parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let query_url = format!("http://{addr}");
    let health_url = format!("{query_url}/health");
    let metrics_url = format!("{query_url}/raft/metrics");
    let self_tag = self_node_tag(node_id);

    tracing::info!(%self_tag, %health_url, "[Iron] [cluster] Raft 查询健康检查地址");
    tracing::info!(%self_tag, %metrics_url, "[Iron] [cluster] Raft 查询指标地址");
    axum::serve(listener, router).await?;

    Ok(())
}

// 查询节点健康状态。
pub async fn health_handler() -> &'static str {
    "ok"
}

// 查询 Raft 指标。
pub async fn metrics_handler(
    State(raft): State<Raft<IronRaftTypeConfig>>,
) -> Json<RaftMetrics<u64, openraft::BasicNode>> {
    Json(raft.metrics().borrow().clone())
}
