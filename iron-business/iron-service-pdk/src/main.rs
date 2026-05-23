// 启动 IronMesh 跑得快服务。
fn main() -> Result<(), iron_core_cluster::IronClusterError> {
    iron_core_cluster::run_worker_from_local_toml(iron_core_cluster::IronClusterServiceKind::Pdk)
}
