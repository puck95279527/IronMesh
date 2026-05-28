use tracing_subscriber::fmt::time::LocalTime;

// 初始化集群实验进程日志。
pub fn init_cluster_process_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(LocalTime::rfc_3339())
        .with_max_level(tracing::Level::DEBUG)
        .init();
}
