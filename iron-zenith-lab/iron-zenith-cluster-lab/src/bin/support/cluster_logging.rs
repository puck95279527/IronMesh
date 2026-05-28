use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::LocalTime;

// 初始化集群实验进程日志。
pub fn init_cluster_process_logging() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_timer(LocalTime::rfc_3339())
        .init();
}
