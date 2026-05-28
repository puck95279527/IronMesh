use std::net::SocketAddr;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use openraft::Raft;
use openraft::RaftMetrics;

use crate::raft::iron_raft_log_tag::self_tag as self_node_tag;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;
use crate::raft::storage::iron_raft_state_machine_store::IronRaftStateMachineStore;

// IronMesh Raft 查询 HTTP 服务共享状态。
#[derive(Clone)]
struct IronRaftQueryHttpState<S>
where
    S: IronRaftStateMachineData,
{
    raft: Raft<IronRaftTypeConfig<S>>, // 当前节点 Raft 句柄。
    state_machine_store: IronRaftStateMachineStore<S>, // 当前节点状态机存储。
}

// 启动 Raft 查询 HTTP 服务。
pub async fn start_query_http_with_addr<S>(
    node_id: u64,
    query_addr: String,
    raft: Raft<IronRaftTypeConfig<S>>,
    state_machine_store: IronRaftStateMachineStore<S>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: IronRaftStateMachineData,
{
    if query_addr.is_empty() {
        return Ok(());
    }

    let query_state = IronRaftQueryHttpState {
        raft,
        state_machine_store,
    };
    let router = Router::new()
        .route("/health", get(health_handler))
        .route("/raft/metrics", get(metrics_handler::<S>))
        .route("/raft/data", get(data_handler::<S>))
        .with_state(query_state);

    let addr = query_addr.parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let query_url = format!("http://{addr}");
    let health_url = format!("{query_url}/health");
    let metrics_url = format!("{query_url}/raft/metrics");
    let data_url = format!("{query_url}/raft/data");
    let self_tag = self_node_tag(node_id);

    tracing::info!(%self_tag, %health_url, "[Iron] [cluster] Raft 查询健康检查地址");
    tracing::info!(%self_tag, %metrics_url, "[Iron] [cluster] Raft 查询指标地址");
    tracing::info!(%self_tag, %data_url, "[Iron] [cluster] Raft 状态机数据查询地址");
    axum::serve(listener, router).await?;

    Ok(())
}

// 查询节点健康状态。
pub async fn health_handler() -> &'static str {
    "ok"
}

// 查询 Raft 指标。
async fn metrics_handler<S>(
    State(query_state): State<IronRaftQueryHttpState<S>>,
) -> Json<RaftMetrics<u64, openraft::BasicNode>>
where
    S: IronRaftStateMachineData,
{
    Json(query_state.raft.metrics().borrow().clone())
}

// 查询当前节点已经 apply 的完整状态机容器数据。
async fn data_handler<S>(State(query_state): State<IronRaftQueryHttpState<S>>) -> Json<S>
where
    S: IronRaftStateMachineData,
{
    Json(
        query_state
            .state_machine_store
            .state_machine
            .lock()
            .await
            .clone(),
    )
}
