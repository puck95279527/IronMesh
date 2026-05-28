use std::net::SocketAddr;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use openraft::Raft;
use openraft::RaftMetrics;

use crate::raft::IronTypeConfig;

// IronMesh Raft 查询 HTTP 服务共享状态。
#[derive(Clone)]
struct IronRaftQueryState {
    raft: Raft<IronTypeConfig>, // 当前节点 Raft 句柄。
}

// 启动 Raft 查询 HTTP 服务。
pub async fn start_query_http_with_addr(
    node_id: u64,
    query_addr: String,
    raft: Raft<IronTypeConfig>,
) -> anyhow::Result<()> {
    if query_addr.is_empty() {
        return Ok(());
    }

    let query_state = IronRaftQueryState { raft };
    let router = Router::new()
        .route("/raft/metrics", get(metrics_handler))
        .with_state(query_state);

    let addr = query_addr.parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let metrics_url = format!("http://{addr}/raft/metrics");

    tracing::info!(
        node_id,
        %metrics_url,
        "[Iron] [cluster] Raft 查询指标地址"
    );

    axum::serve(listener, router).await?;
    Ok(())
}

// 查询 Raft 指标。
async fn metrics_handler(
    State(query_state): State<IronRaftQueryState>,
) -> Json<RaftMetrics<u64, openraft::BasicNode>> {
    Json(query_state.raft.metrics().borrow().clone())
}
