// 集群核心错误模型。

use openraft::error::CheckIsLeaderError;
use openraft::error::ClientWriteError;
use openraft::error::InitializeError;
use openraft::error::RaftError;
use openraft::impls::BasicNode;
use std::path::PathBuf;
use thiserror::Error;

// 集群核心错误类型。
#[derive(Debug, Error)]
pub enum ClusterError {
    // 文件或网络监听错误。
    #[error("集群 IO 错误: {0}")]
    Io(#[from] std::io::Error),
    // 环境变量读取错误。
    #[error("集群环境变量错误: {0}")]
    EnvVar(#[from] std::env::VarError),
    // TOML 配置解析错误。
    #[error("集群 TOML 配置解析错误: {0}")]
    Toml(#[from] toml::de::Error),
    // JSON 编解码错误。
    #[error("集群 JSON 编解码错误: {0}")]
    SerdeJson(#[from] serde_json::Error),
    // 网络监听地址解析错误。
    #[error("集群监听地址解析错误: {0}")]
    AddrParse(#[from] std::net::AddrParseError),
    // OpenRaft 配置错误。
    #[error("集群 Raft 配置错误: {0}")]
    RaftConfig(String),
    // OpenRaft 致命错误。
    #[error("集群 Raft 致命错误: {0}")]
    RaftFatal(String),
    // OpenRaft 初始化错误。
    #[error("集群 Raft 初始化错误: {0}")]
    RaftInitialize(String),
    // OpenRaft 写入错误。
    #[error("集群 Raft 写入错误: {0}")]
    RaftWrite(String),
    // OpenRaft 线性读错误。
    #[error("集群 Raft 线性读错误: {0}")]
    RaftRead(String),
    // 种子配置文件未找到。
    #[error("没有找到集群种子配置文件: {0}")]
    SeedConfigNotFound(PathBuf),
    // 运行目录无法从构建输出目录推导。
    #[error("无法从构建输出目录推导服务运行目录: {0}")]
    RuntimeDirNotFound(PathBuf),
    // TCP 帧类型无法识别。
    #[error("集群 TCP 帧类型无效: {kind}")]
    InvalidFrameKind {
        kind: u16, // 无法识别的 TCP 帧类型编码。
    },
    // TCP 协议消息不符合预期。
    #[error("集群 TCP 协议错误: {0}")]
    Protocol(String),
}

impl From<openraft::ConfigError> for ClusterError {
    // 转换 OpenRaft 配置错误。
    fn from(value: openraft::ConfigError) -> Self {
        Self::RaftConfig(value.to_string())
    }
}

impl From<openraft::error::Fatal<u64>> for ClusterError {
    // 转换 OpenRaft 致命错误。
    fn from(value: openraft::error::Fatal<u64>) -> Self {
        Self::RaftFatal(value.to_string())
    }
}

impl From<RaftError<u64, InitializeError<u64, BasicNode>>> for ClusterError {
    // 转换 OpenRaft 初始化错误。
    fn from(value: RaftError<u64, InitializeError<u64, BasicNode>>) -> Self {
        Self::RaftInitialize(value.to_string())
    }
}

impl From<RaftError<u64, ClientWriteError<u64, BasicNode>>> for ClusterError {
    // 转换 OpenRaft 写入错误。
    fn from(value: RaftError<u64, ClientWriteError<u64, BasicNode>>) -> Self {
        Self::RaftWrite(value.to_string())
    }
}

impl From<RaftError<u64, CheckIsLeaderError<u64, BasicNode>>> for ClusterError {
    // 转换 OpenRaft 线性读错误。
    fn from(value: RaftError<u64, CheckIsLeaderError<u64, BasicNode>>) -> Self {
        Self::RaftRead(value.to_string())
    }
}
