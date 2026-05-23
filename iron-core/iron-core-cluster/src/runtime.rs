// 集群运行时组合逻辑。

use crate::model::IronClusterCommand;
use crate::model::IronClusterCommandResult;
use crate::model::IronClusterEndpointProtocol;
use crate::model::IronClusterEndpointRecord;
use crate::model::IronClusterError;
use crate::model::IronClusterFrameKind;
use crate::model::IronClusterRegistryConfig;
use crate::model::IronClusterRegistryNodeConfig;
use crate::model::IronClusterRegistryRuntimeNode;
use crate::model::IronClusterServiceRecord;
use crate::model::IronClusterState;
use crate::model::IronClusterWorkerConfig;
use crate::model::IronRaft;
use crate::model::IronRaftNetworkFactory;
use crate::model::IronRaftStore;
use openraft::Config;
use openraft::impls::BasicNode;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::VoteRequest;
use std::collections::BTreeMap;
use std::future::pending;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tracing::{error, info, warn};

// 启动注册中心集群。
pub(crate) async fn start_registry_cluster(
    config: IronClusterRegistryConfig,
) -> Result<(), IronClusterError> {
    let tcp_listeners = bind_registry_tcp_listeners(&config.registry_nodes).await?;
    let http_addr: SocketAddr = config.debug_http_addr.parse()?;
    let http_listener = TcpListener::bind(http_addr).await?;
    let nodes = build_registry_nodes(&config).await?;

    for (node, listener) in nodes.clone().into_iter().zip(tcp_listeners) {
        let all_nodes = nodes.clone();
        tokio::spawn(async move {
            if let Err(error) = start_registry_tcp_listener(listener, node, all_nodes).await {
                error!(error = %error, "注册中心 TCP 监听退出");
            }
        });
    }

    let http_nodes = nodes.clone();
    tokio::spawn(async move {
        if let Err(error) = start_registry_debug_http(http_listener, http_addr, http_nodes).await {
            error!(error = %error, "注册中心验证 HTTP 监听退出");
        }
    });

    info!(
        cluster_id = %config.cluster_id,
        debug_http_addr = %config.debug_http_addr,
        "注册中心集群已启动"
    );

    pending::<()>().await;
    Ok(())
}

// 启动工作节点。
pub(crate) async fn start_worker(config: IronClusterWorkerConfig) -> Result<(), IronClusterError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;
    let record = worker_service_record(&config, local_addr);

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((_stream, _addr)) => {}
                Err(error) => {
                    warn!(error = %error, "工作节点 TCP 端口接收连接失败");
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    });

    info!(
        cluster_id = %config.cluster_id,
        node_id = %config.node_id,
        service_name = %config.service_name,
        worker_tcp_addr = %local_addr,
        "工作节点已启动"
    );

    loop {
        for registry_node in &config.registry_nodes {
            match maintain_registry_connection(&registry_node.tcp_addr, &record).await {
                Ok(()) => {}
                Err(error) => {
                    warn!(
                        error = %error,
                        registry_addr = %registry_node.tcp_addr,
                        "工作节点连接注册中心失败"
                    );
                }
            }
            sleep(Duration::from_millis(300)).await;
        }
    }
}

// 初始化集群日志输出。
pub(crate) fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

// 绑定注册中心 TCP 监听器。
async fn bind_registry_tcp_listeners(
    registry_nodes: &[IronClusterRegistryNodeConfig],
) -> Result<Vec<TcpListener>, IronClusterError> {
    let mut listeners = Vec::new();

    for node in registry_nodes {
        listeners.push(TcpListener::bind(&node.tcp_addr).await?);
    }

    Ok(listeners)
}

// 创建注册中心 Raft 节点列表。
async fn build_registry_nodes(
    config: &IronClusterRegistryConfig,
) -> Result<Vec<IronClusterRegistryRuntimeNode>, IronClusterError> {
    let mut nodes = Vec::new();
    let members = registry_members(&config.registry_nodes);

    for node_config in &config.registry_nodes {
        let store = IronRaftStore::default();
        let raft_config = Config {
            cluster_name: config.cluster_id.clone(),
            ..Default::default()
        }
        .validate()?;
        let network = IronRaftNetworkFactory {
            cluster_token: config.cluster_token.clone(),
        };
        let raft = IronRaft::new(
            node_config.raft_node_id,
            Arc::new(raft_config),
            network,
            store.clone(),
            store.clone(),
        )
        .await?;

        if let Err(error) = raft.initialize(members.clone()).await {
            warn!(error = %error, raft_node_id = node_config.raft_node_id, "注册中心 Raft 初始化返回错误");
        }

        nodes.push(IronClusterRegistryRuntimeNode {
            raft_node_id: node_config.raft_node_id,
            tcp_addr: node_config.tcp_addr.clone(),
            raft,
            store,
        });
    }

    Ok(nodes)
}

// 生成注册中心 Raft 初始成员。
fn registry_members(registry_nodes: &[IronClusterRegistryNodeConfig]) -> BTreeMap<u64, BasicNode> {
    registry_nodes
        .iter()
        .map(|node| (node.raft_node_id, BasicNode::new(node.tcp_addr.clone())))
        .collect()
}

// 启动单个注册中心 TCP 监听。
async fn start_registry_tcp_listener(
    listener: TcpListener,
    node: IronClusterRegistryRuntimeNode,
    all_nodes: Vec<IronClusterRegistryRuntimeNode>,
) -> Result<(), IronClusterError> {
    info!(
        raft_node_id = node.raft_node_id,
        tcp_addr = %node.tcp_addr,
        "注册中心 TCP 节点已监听"
    );

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let current_node = node.clone();
        let cluster_nodes = all_nodes.clone();
        tokio::spawn(async move {
            if let Err(error) =
                handle_registry_connection(stream, current_node, cluster_nodes).await
            {
                warn!(error = %error, peer_addr = %peer_addr, "注册中心 TCP 连接处理失败");
            }
        });
    }
}

// 处理注册中心 TCP 连接。
async fn handle_registry_connection(
    mut stream: TcpStream,
    current_node: IronClusterRegistryRuntimeNode,
    all_nodes: Vec<IronClusterRegistryRuntimeNode>,
) -> Result<(), IronClusterError> {
    let (kind, body) = crate::tcp::read_frame(&mut stream).await?;

    match kind {
        IronClusterFrameKind::RegisterService => {
            let record = serde_json::from_slice::<IronClusterServiceRecord>(&body)?;
            handle_worker_connection(stream, record, all_nodes).await
        }
        IronClusterFrameKind::RaftAppend => {
            let request = serde_json::from_slice::<
                AppendEntriesRequest<crate::model::IronRaftTypeConfig>,
            >(&body)?;
            let response = current_node
                .raft
                .append_entries(request)
                .await
                .map_err(|error| IronClusterError::RaftWrite(error.to_string()))?;
            crate::tcp::write_json_frame(&mut stream, IronClusterFrameKind::RaftAppend, &response)
                .await
        }
        IronClusterFrameKind::RaftVote => {
            let request = serde_json::from_slice::<VoteRequest<u64>>(&body)?;
            let response = current_node
                .raft
                .vote(request)
                .await
                .map_err(|error| IronClusterError::RaftWrite(error.to_string()))?;
            crate::tcp::write_json_frame(&mut stream, IronClusterFrameKind::RaftVote, &response)
                .await
        }
        _ => Err(IronClusterError::Protocol(
            "注册中心收到未知首帧".to_string(),
        )),
    }
}

// 处理工作节点注册长连接。
async fn handle_worker_connection(
    mut stream: TcpStream,
    record: IronClusterServiceRecord,
    all_nodes: Vec<IronClusterRegistryRuntimeNode>,
) -> Result<(), IronClusterError> {
    write_command_to_any_raft(
        &all_nodes,
        IronClusterCommand::RegisterService(record.clone()),
    )
    .await?;
    info!(service_name = %record.service_name, node_id = %record.node_id, "工作节点注册成功");

    loop {
        match crate::tcp::read_frame(&mut stream).await {
            Ok((IronClusterFrameKind::Heartbeat, _)) => {}
            Ok((_kind, _body)) => {
                return Err(IronClusterError::Protocol(
                    "工作节点长连接收到非心跳帧".to_string(),
                ));
            }
            Err(IronClusterError::Io(error)) if error.kind() == ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
    }

    write_command_to_any_raft(
        &all_nodes,
        IronClusterCommand::UnregisterService {
            node_id: record.node_id.clone(),
            service_name: record.service_name.clone(),
        },
    )
    .await?;
    info!(service_name = %record.service_name, node_id = %record.node_id, "工作节点已下线");

    Ok(())
}

// 向任意可用 Raft 节点提交命令。
async fn write_command_to_any_raft(
    nodes: &[IronClusterRegistryRuntimeNode],
    command: IronClusterCommand,
) -> Result<IronClusterCommandResult, IronClusterError> {
    for _ in 0..30 {
        if let Some(leader_id) = first_known_leader(nodes).await
            && let Some(node) = nodes.iter().find(|node| node.raft_node_id == leader_id)
            && let Ok(response) = node.raft.client_write(command.clone()).await
        {
            return Ok(response.data);
        }

        for node in nodes {
            if let Ok(response) = node.raft.client_write(command.clone()).await {
                return Ok(response.data);
            }
        }

        sleep(Duration::from_millis(100)).await;
    }

    Err(IronClusterError::RaftWrite(
        "没有可用的注册中心 Raft leader".to_string(),
    ))
}

// 查找任意节点当前已知 leader。
async fn first_known_leader(nodes: &[IronClusterRegistryRuntimeNode]) -> Option<u64> {
    for node in nodes {
        if let Some(leader_id) = node.raft.current_leader().await {
            return Some(leader_id);
        }
    }

    None
}

// 启动注册中心验证 HTTP 服务。
async fn start_registry_debug_http(
    listener: TcpListener,
    http_addr: SocketAddr,
    nodes: Vec<IronClusterRegistryRuntimeNode>,
) -> Result<(), IronClusterError> {
    let app = crate::http::build_registry_debug_http_router(nodes);

    info!(http_addr = %http_addr, "注册中心验证 HTTP 已监听");
    axum::serve(listener, app).await?;
    Ok(())
}

// 维护工作节点到注册中心的长连接。
async fn maintain_registry_connection(
    registry_addr: &str,
    record: &IronClusterServiceRecord,
) -> Result<(), IronClusterError> {
    let mut stream = TcpStream::connect(registry_addr).await?;
    crate::tcp::write_json_frame(&mut stream, IronClusterFrameKind::RegisterService, record)
        .await?;

    loop {
        sleep(Duration::from_millis(500)).await;
        crate::tcp::write_json_frame(&mut stream, IronClusterFrameKind::Heartbeat, record).await?;
    }
}

// 创建工作节点服务注册记录。
fn worker_service_record(
    config: &IronClusterWorkerConfig,
    local_addr: SocketAddr,
) -> IronClusterServiceRecord {
    IronClusterServiceRecord {
        node_id: config.node_id.clone(),
        service_name: config.service_name.clone(),
        state: IronClusterState::Healthy,
        endpoints: vec![IronClusterEndpointRecord {
            name: "cluster-tcp".to_string(),
            protocol: IronClusterEndpointProtocol::Tcp,
            host: local_addr.ip().to_string(),
            port: local_addr.port(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::IronClusterRegistry;

    // 验证注册表可以注册服务。
    #[test]
    fn registry_can_register_service() {
        let mut registry = IronClusterRegistry::default();
        let result = registry.apply_command(IronClusterCommand::RegisterService(test_record()));

        assert_eq!(result.metadata_version, 1);
        assert_eq!(registry.services.len(), 1);
    }

    // 验证注册表可以标记服务下线。
    #[test]
    fn registry_can_unregister_service() {
        let mut registry = IronClusterRegistry::default();
        registry.apply_command(IronClusterCommand::RegisterService(test_record()));
        registry.apply_command(IronClusterCommand::UnregisterService {
            node_id: "iron-gateway-1".to_string(),
            service_name: "iron-gateway".to_string(),
        });

        let record = registry
            .services
            .get("iron-gateway-1:iron-gateway")
            .expect("服务注册记录不存在");
        assert_eq!(record.state, IronClusterState::Offline);
    }

    // 构造测试服务注册记录。
    fn test_record() -> IronClusterServiceRecord {
        IronClusterServiceRecord {
            node_id: "iron-gateway-1".to_string(),
            service_name: "iron-gateway".to_string(),
            state: IronClusterState::Healthy,
            endpoints: Vec::new(),
        }
    }
}
