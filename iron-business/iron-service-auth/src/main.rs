// 启动 IronMesh 登录注册服务。
fn main() -> Result<(), iron_core_cluster::ClusterError> {
    iron_core_cluster::run_worker_from_local_toml(iron_core_cluster::BizServiceKind::Auth)
}
