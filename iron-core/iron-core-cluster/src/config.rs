// 集群配置读取与运行目录配置复制。

use crate::model::IronClusterConfig;
use crate::model::IronClusterError;
use crate::model::IronClusterSeedConfig;
use crate::model::IronClusterServiceKind;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

impl IronClusterConfig {
    // 从环境变量和种子节点配置组合集群启动配置。
    pub(crate) fn from_env_and_seed_config(
        service_kind: IronClusterServiceKind,
        seed_config: IronClusterSeedConfig,
    ) -> Result<Self, IronClusterError> {
        let default_raft_node_id = service_kind.default_raft_node_id();
        let raft_node_id = read_env_or_default("IRON_RAFT_NODE_ID", default_raft_node_id)?;
        let default_http_addr = default_http_addr(service_kind, &seed_config)?;

        Ok(Self {
            cluster_id: read_env_or_string("IRON_CLUSTER_ID", "ironmesh-local"),
            raft_node_id,
            node_id: read_env_or_string("IRON_NODE_ID", service_kind.default_node_id()),
            node_role: service_kind.node_role(),
            service_name: service_kind.service_name().to_string(),
            http_addr: read_env_or_string("IRON_HTTP_ADDR", &default_http_addr),
            cluster_token: read_env_or_string("IRON_CLUSTER_TOKEN", "ironmesh-dev-token"),
            peers: seed_config.peers,
        })
    }
}

// 启动指定服务，并从可执行文件旁边读取集群种子 TOML。
pub(crate) fn run_cluster_service_from_local_toml(
    service_kind: IronClusterServiceKind,
) -> Result<(), IronClusterError> {
    crate::runtime::init_tracing();

    let seed_config_path = local_seed_config_path()?;
    let seed_config = IronClusterSeedConfig::from_toml_file(seed_config_path)?;
    let config = IronClusterConfig::from_env_and_seed_config(service_kind, seed_config)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let cluster_runtime = crate::model::IronClusterRuntime::new(config).await?;
        cluster_runtime.start().await
    })
}

// 复制集群种子配置到服务运行目录。
pub(crate) fn copy_cluster_seed_config_to_runtime_dir() -> Result<(), IronClusterError> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let source = find_cluster_seed_config(&manifest_dir)?;
    let target_dir = find_runtime_dir(&out_dir)?;
    let target = target_dir.join(IRON_CLUSTER_SEED_FILE_NAME);

    println!("cargo:rerun-if-changed={}", source.display());
    fs::copy(source, target)?;

    Ok(())
}

// 读取字符串环境变量，没有设置时使用默认值。
fn read_env_or_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

// 读取数字环境变量，没有设置时使用默认值。
fn read_env_or_default(name: &str, default: u64) -> Result<u64, IronClusterError> {
    match env::var(name) {
        Ok(value) => Ok(value
            .parse()
            .map_err(|_| IronClusterError::InvalidNumberEnv {
                name: name.to_string(),
                value,
            })?),
        Err(_) => Ok(default),
    }
}

// 根据服务类型和种子节点配置推导默认监听地址。
fn default_http_addr(
    service_kind: IronClusterServiceKind,
    seed_config: &IronClusterSeedConfig,
) -> Result<String, IronClusterError> {
    let raft_node_id = service_kind.default_raft_node_id();
    let Some(peer) = seed_config
        .peers
        .iter()
        .find(|peer| peer.raft_node_id == raft_node_id)
    else {
        return Ok("127.0.0.1:0".to_string());
    };

    http_url_to_addr(&peer.http_url)
}

// 把 HTTP URL 转换为监听地址。
pub(crate) fn http_url_to_addr(http_url: &str) -> Result<String, IronClusterError> {
    http_url
        .strip_prefix("http://")
        .or_else(|| http_url.strip_prefix("https://"))
        .map(|addr| addr.trim_end_matches('/').to_string())
        .ok_or_else(|| IronClusterError::InvalidPeerUrl {
            http_url: http_url.to_string(),
        })
}

// 查找仓库根目录下的集群种子配置。
fn find_cluster_seed_config(start_dir: &Path) -> Result<PathBuf, IronClusterError> {
    for dir in start_dir.ancestors() {
        let path = dir.join("config").join(IRON_CLUSTER_SEED_FILE_NAME);
        if path.exists() {
            return Ok(path);
        }
    }

    Err(IronClusterError::SeedConfigNotFound(
        start_dir.join("config").join(IRON_CLUSTER_SEED_FILE_NAME),
    ))
}

// 根据构建输出目录推导二进制运行目录。
fn find_runtime_dir(out_dir: &Path) -> Result<PathBuf, IronClusterError> {
    let profile = env::var("PROFILE")?;

    for dir in out_dir.ancestors() {
        if dir.file_name() == Some(OsStr::new(&profile)) {
            return Ok(dir.to_path_buf());
        }
    }

    Err(IronClusterError::RuntimeDirNotFound(out_dir.to_path_buf()))
}

// 返回可执行文件旁边的集群种子配置路径。
fn local_seed_config_path() -> Result<PathBuf, IronClusterError> {
    let executable = env::current_exe()?;
    let Some(dir) = executable.parent() else {
        return Err(IronClusterError::SeedConfigNotFound(executable));
    };

    let path = dir.join(IRON_CLUSTER_SEED_FILE_NAME);
    if path.exists() {
        Ok(path)
    } else {
        Err(IronClusterError::SeedConfigNotFound(path))
    }
}

// 集群种子配置文件名。
const IRON_CLUSTER_SEED_FILE_NAME: &str = "cluster-seeds.toml";
