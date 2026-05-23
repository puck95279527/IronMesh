// 集群核心对外公开 API。

use crate::model::IronClusterError;
use crate::model::IronClusterServiceKind;

// 启动注册中心，并从可执行文件旁边读取集群种子 TOML。
pub fn run_registry_cluster_from_local_toml() -> Result<(), IronClusterError> {
    crate::config::run_registry_cluster_from_local_toml()
}

// 启动工作节点，并从可执行文件旁边读取集群种子 TOML。
pub fn run_worker_from_local_toml(
    service_kind: IronClusterServiceKind,
) -> Result<(), IronClusterError> {
    crate::config::run_worker_from_local_toml(service_kind)
}

// 复制集群种子配置到服务运行目录。
pub fn copy_cluster_seed_config_to_runtime_dir() -> Result<(), IronClusterError> {
    crate::config::copy_cluster_seed_config_to_runtime_dir()
}
