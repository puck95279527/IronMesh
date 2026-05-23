// 集群运行时组合逻辑。

use crate::config::http_url_to_addr;
use crate::http::IRON_CLUSTER_TOKEN_HEADER;
use crate::model::IronClusterCommand;
use crate::model::IronClusterCommandResult;
use crate::model::IronClusterConfig;
use crate::model::IronClusterEndpointProtocol;
use crate::model::IronClusterEndpointRecord;
use crate::model::IronClusterError;
use crate::model::IronClusterRuntime;
use crate::model::IronClusterServiceRecord;
use crate::model::IronClusterState;
use crate::model::IronRaft;
use crate::model::IronRaftNetworkFactory;
use crate::model::IronRaftStore;
use openraft::Config;
use openraft::impls::BasicNode;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tracing::{info, warn};

impl IronClusterRuntime {
    // 创建集群运行时。
    pub(crate) async fn new(config: IronClusterConfig) -> Result<Self, IronClusterError> {
        let http_client = reqwest::Client::new();
        let store = IronRaftStore::default();
        let raft_config = Config {
            cluster_name: config.cluster_id.clone(),
            ..Default::default()
        }
        .validate()?;
        let network = IronRaftNetworkFactory {
            cluster_token: config.cluster_token.clone(),
            http_client: http_client.clone(),
        };
        let raft = IronRaft::new(
            config.raft_node_id,
            std::sync::Arc::new(raft_config),
            network,
            store.clone(),
            store.clone(),
        )
        .await?;
        let members = raft_members(&config);
        if let Err(error) = raft.initialize(members).await {
            warn!(error = %error, "集群 Raft 初始化返回错误");
        }

        Ok(Self {
            config,
            raft,
            store,
            http_client,
        })
    }

    // 启动集群运行时。
    pub(crate) async fn start(self) -> Result<(), IronClusterError> {
        let record = self.current_service_record()?;
        let addr: SocketAddr = self.config.http_addr.parse()?;
        let app = crate::http::build_cluster_http_router(
            self.config.cluster_token.clone(),
            self.raft.clone(),
            self.store.clone(),
        );
        let listener = TcpListener::bind(addr).await?;

        info!(
            cluster_id = %self.config.cluster_id,
            node_id = %self.config.node_id,
            service_name = %self.config.service_name,
            http_addr = %self.config.http_addr,
            "集群控制面已启动"
        );

        let runtime = self.clone();
        tokio::spawn(async move {
            runtime.register_until_success(record).await;
        });

        axum::serve(listener, app).await?;
        Ok(())
    }

    // 注册或更新服务。
    async fn register_service(
        &self,
        record: IronClusterServiceRecord,
    ) -> Result<IronClusterCommandResult, IronClusterError> {
        if self.raft.current_leader().await == Some(self.config.raft_node_id) {
            let response = self
                .raft
                .client_write(IronClusterCommand::RegisterService(record.clone()))
                .await?;
            return Ok(response.data);
        }

        self.send_register_to_leader_or_peers(&record).await
    }

    // 创建当前服务的注册记录。
    fn current_service_record(&self) -> Result<IronClusterServiceRecord, IronClusterError> {
        let (host, port) = split_http_addr(&self.config.http_addr)?;

        Ok(IronClusterServiceRecord {
            node_id: self.config.node_id.clone(),
            service_name: self.config.service_name.clone(),
            state: IronClusterState::Healthy,
            endpoints: vec![IronClusterEndpointRecord {
                name: "cluster-http".to_string(),
                protocol: IronClusterEndpointProtocol::Http,
                host,
                port,
            }],
        })
    }

    // 返回需要同步的对端控制面地址。
    fn peer_urls(&self) -> Vec<String> {
        self.config
            .peers
            .iter()
            .filter(|peer| peer.raft_node_id != self.config.raft_node_id)
            .map(|peer| peer.http_url.clone())
            .collect()
    }

    // 向单个种子节点推送当前服务注册记录。
    async fn send_register_to_peer(
        &self,
        peer_url: &str,
        record: &IronClusterServiceRecord,
    ) -> Result<(), IronClusterError> {
        let url = format!("{}/iron/cluster/register", peer_url.trim_end_matches('/'));
        self.http_client
            .post(url)
            .header(IRON_CLUSTER_TOKEN_HEADER, &self.config.cluster_token)
            .json(record)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    // 向当前 leader 或所有种子节点提交服务注册。
    async fn send_register_to_leader_or_peers(
        &self,
        record: &IronClusterServiceRecord,
    ) -> Result<IronClusterCommandResult, IronClusterError> {
        if let Some(leader_id) = self.raft.current_leader().await
            && let Some(peer_url) = self.peer_url_by_raft_node_id(leader_id)
        {
            self.send_register_to_peer(&peer_url, record).await?;
            return Ok(IronClusterCommandResult::default());
        }

        for peer_url in self.peer_urls() {
            if self.send_register_to_peer(&peer_url, record).await.is_ok() {
                return Ok(IronClusterCommandResult::default());
            }
        }

        Err(IronClusterError::RaftWrite(
            "没有可用的 Raft leader 接收服务注册".to_string(),
        ))
    }

    // 根据 Raft 节点 ID 查找对端控制面地址。
    fn peer_url_by_raft_node_id(&self, raft_node_id: u64) -> Option<String> {
        self.config
            .peers
            .iter()
            .find(|peer| peer.raft_node_id == raft_node_id)
            .map(|peer| peer.http_url.clone())
    }

    // 持续重试注册当前服务，直到注册成功或达到重试上限。
    async fn register_until_success(&self, record: IronClusterServiceRecord) {
        for _ in 0..60 {
            match self.register_service(record.clone()).await {
                Ok(_) => {
                    info!(service_name = %record.service_name, "集群服务注册成功");
                    return;
                }
                Err(error) => {
                    warn!(error = %error, "集群服务注册失败，稍后重试");
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }

        warn!(service_name = %record.service_name, "集群服务注册达到重试上限");
    }
}

// 初始化集群日志输出。
pub(crate) fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

// 拆分监听地址中的主机和端口。
fn split_http_addr(http_addr: &str) -> Result<(String, u16), IronClusterError> {
    let addr: SocketAddr = http_addr.parse()?;
    let host = http_url_to_addr(&format!("http://{addr}"))?
        .rsplit_once(':')
        .map(|(host, _)| host.to_string())
        .unwrap_or_else(|| addr.ip().to_string());

    Ok((host, addr.port()))
}

// 生成 Raft 初始成员。
fn raft_members(config: &IronClusterConfig) -> BTreeMap<u64, BasicNode> {
    config
        .peers
        .iter()
        .map(|peer| (peer.raft_node_id, BasicNode::new(peer.http_url.clone())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::IronClusterCommand;
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
