// 集群运行时组合逻辑。

use crate::model::BizService;
use crate::model::BizServiceKind;
use crate::model::ClusterCommand;
use crate::model::ClusterError;
use crate::model::ClusterFrameKind;
use crate::model::ClusterRegistryConfig;
use crate::model::ClusterRegistryNodeConfig;
use crate::model::ClusterRegistryRuntimeNode;
use crate::model::ClusterWorkerConfig;
use crate::model::IronClusterService;
use crate::model::IronRaftNetworkFactory;
use crate::model::IronRaftStore;
use crate::model::RaftServiceRole;
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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tracing::{error, info, warn};

// 启动注册中心集群。
pub(crate) async fn start_registry_cluster(
    config: ClusterRegistryConfig,
) -> Result<(), ClusterError> {
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

    let registry_nodes = nodes.clone();
    let registry_http_addr = config.debug_http_addr.clone();
    tokio::spawn(async move {
        register_registry_nodes_until_success(registry_nodes, registry_http_addr).await;
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
pub(crate) async fn start_worker(config: ClusterWorkerConfig) -> Result<(), ClusterError> {
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
        biz_service_id = %config.biz_service_id,
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
    registry_nodes: &[ClusterRegistryNodeConfig],
) -> Result<Vec<TcpListener>, ClusterError> {
    let mut listeners = Vec::new();

    for node in registry_nodes {
        listeners.push(TcpListener::bind(&node.tcp_addr).await?);
    }

    Ok(listeners)
}

// 创建注册中心 Raft 节点列表。
async fn build_registry_nodes(
    config: &ClusterRegistryConfig,
) -> Result<Vec<ClusterRegistryRuntimeNode>, ClusterError> {
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
        let raft = openraft::Raft::new(
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

        nodes.push(ClusterRegistryRuntimeNode {
            raft_node_id: node_config.raft_node_id,
            tcp_addr: node_config.tcp_addr.clone(),
            raft,
            store,
        });
    }

    Ok(nodes)
}

// 生成注册中心 Raft 初始成员。
fn registry_members(registry_nodes: &[ClusterRegistryNodeConfig]) -> BTreeMap<u64, BasicNode> {
    registry_nodes
        .iter()
        .map(|node| (node.raft_node_id, BasicNode::new(node.tcp_addr.clone())))
        .collect()
}

// 启动单个注册中心 TCP 监听。
async fn start_registry_tcp_listener(
    listener: TcpListener,
    node: ClusterRegistryRuntimeNode,
    all_nodes: Vec<ClusterRegistryRuntimeNode>,
) -> Result<(), ClusterError> {
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
    current_node: ClusterRegistryRuntimeNode,
    all_nodes: Vec<ClusterRegistryRuntimeNode>,
) -> Result<(), ClusterError> {
    let (kind, body) = crate::tcp::read_frame(&mut stream).await?;

    match kind {
        ClusterFrameKind::RegisterService => {
            let record = serde_json::from_slice::<IronClusterService>(&body)?;
            handle_worker_connection(stream, record, all_nodes).await
        }
        ClusterFrameKind::RaftAppend => {
            let request = serde_json::from_slice::<
                AppendEntriesRequest<crate::model::IronRaftTypeConfig>,
            >(&body)?;
            let response = current_node
                .raft
                .append_entries(request)
                .await
                .map_err(|error| ClusterError::RaftWrite(error.to_string()))?;
            crate::tcp::write_json_frame(&mut stream, ClusterFrameKind::RaftAppend, &response).await
        }
        ClusterFrameKind::RaftVote => {
            let request = serde_json::from_slice::<VoteRequest<u64>>(&body)?;
            let response = current_node
                .raft
                .vote(request)
                .await
                .map_err(|error| ClusterError::RaftWrite(error.to_string()))?;
            crate::tcp::write_json_frame(&mut stream, ClusterFrameKind::RaftVote, &response).await
        }
        _ => Err(ClusterError::Protocol("注册中心收到未知首帧".to_string())),
    }
}

// 处理工作节点注册长连接。
async fn handle_worker_connection(
    mut stream: TcpStream,
    service: IronClusterService,
    all_nodes: Vec<ClusterRegistryRuntimeNode>,
) -> Result<(), ClusterError> {
    write_command_to_any_raft(&all_nodes, ClusterCommand::Upsert(service.clone())).await?;
    info!(biz_service_id = %service.biz_service_id, "工作节点注册成功");

    loop {
        match crate::tcp::read_frame(&mut stream).await {
            Ok((ClusterFrameKind::Heartbeat, _)) => {}
            Ok((_kind, _body)) => {
                return Err(ClusterError::Protocol(
                    "工作节点长连接收到非心跳帧".to_string(),
                ));
            }
            Err(ClusterError::Io(error)) if error.kind() == ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
    }

    write_command_to_any_raft(
        &all_nodes,
        ClusterCommand::Offline {
            biz_service_id: service.biz_service_id.clone(),
        },
    )
    .await?;
    info!(biz_service_id = %service.biz_service_id, "工作节点已下线");

    Ok(())
}

// 向任意可用 Raft 节点提交命令。
async fn write_command_to_any_raft(
    nodes: &[ClusterRegistryRuntimeNode],
    command: ClusterCommand,
) -> Result<(), ClusterError> {
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

    Err(ClusterError::RaftWrite(
        "没有可用的注册中心 Raft leader".to_string(),
    ))
}

// 查找任意节点当前已知 leader。
async fn first_known_leader(nodes: &[ClusterRegistryRuntimeNode]) -> Option<u64> {
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
    nodes: Vec<ClusterRegistryRuntimeNode>,
) -> Result<(), ClusterError> {
    let app = crate::http::build_registry_debug_http_router(nodes);

    info!(http_addr = %http_addr, "注册中心验证 HTTP 已监听");
    axum::serve(listener, app).await?;
    Ok(())
}

// 维护工作节点到注册中心的长连接。
async fn maintain_registry_connection(
    registry_addr: &str,
    record: &IronClusterService,
) -> Result<(), ClusterError> {
    let mut stream = TcpStream::connect(registry_addr).await?;
    crate::tcp::write_json_frame(&mut stream, ClusterFrameKind::RegisterService, record).await?;

    loop {
        sleep(Duration::from_millis(500)).await;
        crate::tcp::write_json_frame(&mut stream, ClusterFrameKind::Heartbeat, record).await?;
    }
}

// 创建工作节点服务注册记录。
fn worker_service_record(
    config: &ClusterWorkerConfig,
    local_addr: SocketAddr,
) -> IronClusterService {
    IronClusterService {
        raft_id: None,
        raft_role: None,
        raft_addr: None,
        raft_epoch: None,
        raft_alive_at_ms: None,
        biz_kind: config.biz_kind,
        biz_service_id: config.biz_service_id.clone(),
        biz_services: vec![BizService {
            name: "cluster-tcp".to_string(),
            addr: local_addr.to_string(),
        }],
    }
}

// 持续注册注册中心自身节点。
async fn register_registry_nodes_until_success(
    nodes: Vec<ClusterRegistryRuntimeNode>,
    debug_http_addr: String,
) {
    let services: Vec<_> = nodes
        .iter()
        .map(|node| registry_service_record(node.raft_node_id, &node.tcp_addr, &debug_http_addr))
        .collect();

    for _ in 0..60 {
        let mut all_written = true;
        for service in &services {
            if write_command_to_any_raft(&nodes, ClusterCommand::Upsert(service.clone()))
                .await
                .is_err()
            {
                all_written = false;
            }
        }

        if all_written {
            return;
        }

        sleep(Duration::from_millis(500)).await;
    }
}

// 创建注册中心服务记录。
fn registry_service_record(
    raft_node_id: u64,
    tcp_addr: &str,
    debug_http_addr: &str,
) -> IronClusterService {
    let mut biz_services = vec![BizService {
        name: "raft-tcp".to_string(),
        addr: tcp_addr.to_string(),
    }];

    if raft_node_id == 1 {
        biz_services.push(BizService {
            name: "admin-http".to_string(),
            addr: debug_http_addr.to_string(),
        });
    }

    IronClusterService {
        raft_id: Some(raft_node_id),
        raft_role: Some(RaftServiceRole::Learner),
        raft_addr: Some(tcp_addr.to_string()),
        raft_epoch: Some(1),
        raft_alive_at_ms: Some(now_ms()),
        biz_kind: BizServiceKind::Registry,
        biz_service_id: format!("registry-{raft_node_id}"),
        biz_services,
    }
}

// 返回当前毫秒时间戳。
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    // 验证注册表可以注册服务。
    #[test]
    fn registry_can_register_service() {
        let mut registry = BTreeMap::default();
        ClusterCommand::Upsert(test_record()).apply_to(&mut registry);

        assert_eq!(registry.len(), 1);
        assert!(registry.contains_key("gate-1"));
    }

    // 验证注册表可以标记服务下线。
    #[test]
    fn registry_can_unregister_service() {
        let mut registry = BTreeMap::default();
        ClusterCommand::Upsert(test_record()).apply_to(&mut registry);
        ClusterCommand::Offline {
            biz_service_id: "gate-1".to_string(),
        }
        .apply_to(&mut registry);

        assert!(registry.is_empty());
    }

    // 验证注册中心服务记录包含 Raft 字段。
    #[test]
    fn registry_service_has_raft_fields() {
        let service = registry_service_record(1, "127.0.0.1:6001", "127.0.0.1:8888");

        assert_eq!(service.biz_kind, BizServiceKind::Registry);
        assert_eq!(service.raft_id, Some(1));
        assert!(service.raft_addr.is_some());
    }

    // 验证工作节点服务记录不包含 Raft 字段。
    #[test]
    fn worker_service_has_no_raft_fields() {
        let config = ClusterWorkerConfig {
            cluster_id: "ironmesh-local".to_string(),
            cluster_token: "token".to_string(),
            biz_kind: BizServiceKind::GamePdk,
            biz_service_id: "game_pdk-1001".to_string(),
            registry_nodes: Vec::new(),
        };
        let local_addr: SocketAddr = "127.0.0.1:9000".parse().expect("监听地址无效");
        let service = worker_service_record(&config, local_addr);

        assert_eq!(service.biz_kind, BizServiceKind::GamePdk);
        assert_eq!(service.raft_id, None);
        assert_eq!(service.raft_addr, None);
    }

    // 构造测试服务注册记录。
    fn test_record() -> IronClusterService {
        IronClusterService {
            raft_id: None,
            raft_role: None,
            raft_addr: None,
            raft_epoch: None,
            raft_alive_at_ms: None,
            biz_kind: BizServiceKind::Gate,
            biz_service_id: "gate-1".to_string(),
            biz_services: Vec::new(),
        }
    }
}
