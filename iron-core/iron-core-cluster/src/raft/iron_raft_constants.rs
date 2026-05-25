use std::time::Duration;

// ==================== Raft 核心参数 ====================

// Raft 心跳间隔，单位为毫秒。
pub(crate) const RAFT_HEARTBEAT_INTERVAL: u64 = 500;

// Raft 选举最小超时时间，单位为毫秒。
pub(crate) const RAFT_ELECTION_TIMEOUT_MIN: u64 = 1500;

// Raft 选举最大超时时间，单位为毫秒。
pub(crate) const RAFT_ELECTION_TIMEOUT_MAX: u64 = 3000;

// ==================== 集群启动参数 ====================

// 集群启动流程等待下一轮加入探测的间隔。
pub(crate) const CLUSTER_STARTUP_RETRY_INTERVAL: Duration = Duration::from_millis(800);

// 集群启动流程遇到初始化错误后的重试间隔。
pub(crate) const CLUSTER_STARTUP_ERROR_RETRY_INTERVAL: Duration = Duration::from_millis(500);

// 初始化最小 Raft 集群前的短暂等待时间。
pub(crate) const CLUSTER_INITIALIZE_DELAY: Duration = Duration::from_millis(200);

// 起盘节点扫描注册节点没有进展时的短暂等待时间。
pub(crate) const BOOT_NODE_JOIN_EMPTY_ROUND_INTERVAL: Duration = Duration::from_millis(100);

// 注册节点加入为 learner 或 voter 失败后的重试间隔。
pub(crate) const BOOT_NODE_JOIN_RETRY_INTERVAL: Duration = Duration::from_secs(1);

// 注册节点加入为 learner 或 voter 的最大重试次数。
pub(crate) const BOOT_NODE_JOIN_RETRY_LIMIT: usize = 5;

// 节点加入 RPC 成功后等待本地 Raft 状态就绪的超时时间。
pub(crate) const JOIN_LOCAL_READY_TIMEOUT: Duration = Duration::from_secs(5);

// learner 断线后移出 membership 的最大尝试次数。
pub(crate) const LEARNER_REMOVE_RETRY_LIMIT: usize = 3;

// learner 断线后移出 membership 失败时的短暂重试间隔。
pub(crate) const LEARNER_REMOVE_RETRY_INTERVAL: Duration = Duration::from_millis(200);

// ==================== TCP 帧安全参数 ====================

// TCP frame 最大 body 长度，超过后直接拒绝读取或写入。
pub(crate) const MAX_FRAME_BODY_LEN: usize = 16 * 1024 * 1024;

// TCP frame 单次读取超时时间。
pub(crate) const TCP_FRAME_READ_TIMEOUT: Duration = Duration::from_secs(5);

// TCP frame 等待下一帧头的空闲超时时间。
pub(crate) const TCP_FRAME_HEADER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

// TCP frame 单次写入和 flush 超时时间。
pub(crate) const TCP_FRAME_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

// ==================== TCP 连接参数 ====================

// Raft TCP 服务端最大并发连接数。
pub(crate) const MAX_TCP_CONNECTIONS: usize = 256;

// 节点加入 TCP RPC 超时时间。
pub(crate) const JOIN_NODE_TIMEOUT: Duration = Duration::from_millis(500);

// 客户端业务写入 TCP RPC 超时时间。
pub(crate) const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

// 节点 TCP 可达性探测超时时间。
pub(crate) const PEER_REACHABLE_TIMEOUT: Duration = Duration::from_millis(100);
