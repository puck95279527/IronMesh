use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::Config;
use openraft::Raft;

use crate::raft::model::iron_raft_log_store::IronRaftLogStore;
use crate::raft::model::iron_raft_state_machine_store::IronRaftStateMachineStore;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::network::iron_raft_http_server::IronRaftHttpServer;
use crate::raft::network::iron_raft_network_factory::IronRaftNetworkFactory;

// Raft 能力模块入口。
pub mod model;
pub mod network;

// 启动一个最小 Raft 节点。
pub async fn start_iron_raft_node(
    node_id: u64,
    http_addr: String,
    query_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        }
        .validate()?,
    );

    let raft = Raft::<IronRaftTypeConfig>::new(
        node_id,
        config,
        IronRaftNetworkFactory::default(),
        IronRaftLogStore::default(),
        IronRaftStateMachineStore::default(),
    )
    .await?;

    let init_raft = raft.clone();
    let query_raft = raft.clone();
    let router = IronRaftHttpServer::router(raft);

    let addr = http_addr.parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!(node_id, %http_addr, "启动 IronMesh Raft 节点");
    if node_id == 1 {
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            if let Err(error) = init_raft.initialize(initial_members()).await {
                tracing::warn!(%error, "初始化 IronMesh Raft 集群失败");
            }
        });
    }

    if query_port != 0 {
        tokio::spawn(async move {
            if let Err(error) = crate::http::iron_raft_query::start_query_http(node_id, query_port, query_raft).await {
                tracing::warn!(%error, "IronMesh Raft 查询 HTTP 服务退出");
            }
        });
    }

    axum::serve(listener, router).await?;

    Ok(())
}

// 构建写死的初始成员。
fn initial_members() -> BTreeMap<u64, openraft::BasicNode> {
    BTreeMap::from([
        (1, openraft::BasicNode::new("127.0.0.1:5001")),
        (2, openraft::BasicNode::new("127.0.0.1:5002")),
        (3, openraft::BasicNode::new("127.0.0.1:5003")),
    ])
}
