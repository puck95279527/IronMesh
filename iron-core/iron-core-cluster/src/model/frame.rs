// 集群 TCP 帧数据模型。

use serde::{Deserialize, Serialize};

// 集群 TCP 帧类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IronClusterFrameKind {
    RegisterService,   // 工作节点注册服务。
    Heartbeat,         // 工作节点心跳。
    UnregisterService, // 工作节点下线服务。
    RaftAppend,        // Raft 日志复制请求。
    RaftVote,          // Raft 投票请求。
    Error,             // 协议错误响应。
}

impl IronClusterFrameKind {
    // 返回 TCP 帧类型编码。
    pub(crate) fn code(self) -> u16 {
        match self {
            Self::RegisterService => 1,
            Self::Heartbeat => 2,
            Self::UnregisterService => 3,
            Self::RaftAppend => 10,
            Self::RaftVote => 11,
            Self::Error => u16::MAX,
        }
    }

    // 从 TCP 帧类型编码解析帧类型。
    pub(crate) fn from_code(code: u16) -> Option<Self> {
        match code {
            1 => Some(Self::RegisterService),
            2 => Some(Self::Heartbeat),
            3 => Some(Self::UnregisterService),
            10 => Some(Self::RaftAppend),
            11 => Some(Self::RaftVote),
            u16::MAX => Some(Self::Error),
            _ => None,
        }
    }
}

// 集群 TCP 帧头。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronClusterFrameHeader {
    pub kind: IronClusterFrameKind, // TCP 帧类型。
    pub body_len: u32,              // TCP 帧 body 字节长度。
}
