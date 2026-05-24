// 集群配置读取与运行目录配置复制。

use crate::model::BizServiceKind;
use crate::model::ClusterError;
use crate::model::ClusterRegistryConfig;
use crate::model::ClusterSeedConfig;
use crate::model::ClusterWorkerConfig;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

impl ClusterRegistryConfig {
    // 从环境变量和种子节点配置组合注册中心启动配置。
    pub(crate) fn from_env_and_seed_config(seed_config: ClusterSeedConfig) -> Self {
        Self {
            cluster_id: read_env_or_string("IRON_CLUSTER_ID", "ironmesh-local"),
            cluster_token: read_env_or_string("IRON_CLUSTER_TOKEN", "ironmesh-dev-token"),
            registry_nodes: seed_config.registry_nodes,
            debug_http_addr: read_env_or_string(
                "IRON_REGISTRY_HTTP_ADDR",
                &seed_config.debug_http.http_addr,
            ),
        }
    }
}

impl ClusterWorkerConfig {
    // 从环境变量和种子节点配置组合工作节点启动配置。
    pub(crate) fn from_env_and_seed_config(
        biz_kind: BizServiceKind,
        seed_config: ClusterSeedConfig,
    ) -> Self {
        Self {
            cluster_id: read_env_or_string("IRON_CLUSTER_ID", "ironmesh-local"),
            cluster_token: read_env_or_string("IRON_CLUSTER_TOKEN", "ironmesh-dev-token"),
            biz_kind,
            biz_service_id: read_env_or_string(
                "IRON_BIZ_SERVICE_ID",
                biz_kind.default_biz_service_id(),
            ),
            registry_nodes: seed_config.registry_nodes,
        }
    }
}

// 启动注册中心，并从可执行文件旁边读取集群种子 TOML。
pub(crate) fn run_registry_cluster_from_local_toml() -> Result<(), ClusterError> {
    crate::runtime::init_tracing();

    let seed_config_path = local_seed_config_path()?;
    let seed_config = ClusterSeedConfig::from_toml_file(seed_config_path)?;
    let config = ClusterRegistryConfig::from_env_and_seed_config(seed_config);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(crate::runtime::start_registry_cluster(config))
}

// 启动工作节点，并从可执行文件旁边读取集群种子 TOML。
pub(crate) fn run_worker_from_local_toml(biz_kind: BizServiceKind) -> Result<(), ClusterError> {
    crate::runtime::init_tracing();

    let seed_config_path = local_seed_config_path()?;
    let seed_config = ClusterSeedConfig::from_toml_file(seed_config_path)?;
    let config = ClusterWorkerConfig::from_env_and_seed_config(biz_kind, seed_config);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(crate::runtime::start_worker(config))
}

// 复制集群种子配置到服务运行目录。
pub(crate) fn copy_cluster_seed_config_to_runtime_dir() -> Result<(), ClusterError> {
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

// 查找仓库根目录下的集群种子配置。
fn find_cluster_seed_config(start_dir: &Path) -> Result<PathBuf, ClusterError> {
    for dir in start_dir.ancestors() {
        let path = dir.join("config").join(IRON_CLUSTER_SEED_FILE_NAME);
        if path.exists() {
            return Ok(path);
        }
    }

    Err(ClusterError::SeedConfigNotFound(
        start_dir.join("config").join(IRON_CLUSTER_SEED_FILE_NAME),
    ))
}

// 根据构建输出目录推导二进制运行目录。
fn find_runtime_dir(out_dir: &Path) -> Result<PathBuf, ClusterError> {
    let profile = env::var("PROFILE")?;

    for dir in out_dir.ancestors() {
        if dir.file_name() == Some(OsStr::new(&profile)) {
            return Ok(dir.to_path_buf());
        }
    }

    Err(ClusterError::RuntimeDirNotFound(out_dir.to_path_buf()))
}

// 返回可执行文件旁边的集群种子配置路径。
fn local_seed_config_path() -> Result<PathBuf, ClusterError> {
    let executable = env::current_exe()?;
    let Some(dir) = executable.parent() else {
        return Err(ClusterError::SeedConfigNotFound(executable));
    };

    let path = dir.join(IRON_CLUSTER_SEED_FILE_NAME);
    if path.exists() {
        Ok(path)
    } else {
        Err(ClusterError::SeedConfigNotFound(path))
    }
}

// 集群种子配置文件名。
const IRON_CLUSTER_SEED_FILE_NAME: &str = "cluster-seeds.toml";
