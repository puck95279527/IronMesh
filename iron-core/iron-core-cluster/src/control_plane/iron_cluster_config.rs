use std::time::Duration;

// Raft TCP 单帧允许的最大字节数。
pub const IRON_TCP_MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

// Raft TCP 服务端允许同时处理的最大连接数。
pub(crate) const RAFT_TCP_MAX_CONNECTIONS: usize = 1024;

// Raft TCP 服务端等待读取单个请求的最长时间。
pub(crate) const RAFT_TCP_READ_TIMEOUT: Duration = Duration::from_secs(30);

// Raft TCP 服务端等待写入单个响应的最长时间。
pub(crate) const RAFT_TCP_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

// 节点加入集群失败后的重试间隔。
pub(crate) const CLUSTER_JOIN_RETRY_INTERVAL: Duration = Duration::from_secs(1);

// 节点加入集群 TCP 请求等待响应的最长时间。
pub(crate) const CLUSTER_JOIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

// 本地节点等待 Raft membership 就绪的最长时间。
pub(crate) const JOIN_LOCAL_READY_TIMEOUT: Duration = Duration::from_secs(5);

// learner 断线后尝试移出 membership 的重试间隔。
pub(crate) const LEARNER_REMOVE_RETRY_INTERVAL: Duration = Duration::from_millis(200);

// learner 断线后尝试移出 membership 的最大次数。
pub(crate) const LEARNER_REMOVE_RETRY_LIMIT: usize = 3;

// 探测启动节点 TCP 地址是否可达的最长时间。
pub(crate) const PEER_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

// Raft TCP 网络事件通道容量。
pub(crate) const RAFT_NETWORK_EVENT_CHANNEL_CAPACITY: usize = 1024;
