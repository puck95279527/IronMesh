use time::macros::format_description;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::LocalTime;

// 初始化集群实验进程日志。
pub fn init_cluster_process_logging() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,iron_core_cluster=debug,openraft=warn"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_timer(LocalTime::new(format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3][offset_hour sign:mandatory]:[offset_minute]"
        )))
        .with_target(false)
        .init();
}
