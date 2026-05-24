// 集群核心对外公开 API。

use crate::model::BizServiceKind;
use crate::model::ClusterError;

// 启动注册中心，并从可执行文件旁边读取集群种子 TOML。
pub fn run_registry_cluster_from_local_toml() -> Result<(), ClusterError> {
    crate::core::config::run_registry_cluster_from_local_toml()
}

// 启动工作节点，并从可执行文件旁边读取集群种子 TOML。
pub fn run_worker_from_local_toml(biz_kind: BizServiceKind) -> Result<(), ClusterError> {
    crate::core::config::run_worker_from_local_toml(biz_kind)
}

// 复制集群种子配置到服务运行目录。
pub fn copy_cluster_seed_config_to_runtime_dir() -> Result<(), ClusterError> {
    crate::core::config::copy_cluster_seed_config_to_runtime_dir()
}
