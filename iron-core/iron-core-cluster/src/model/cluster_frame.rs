// 集群 TCP 帧数据结构。

use serde::{Deserialize, Serialize};

// 集群 TCP 帧类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClusterFrameKind {
    RegisterService, // 工作节点注册服务。
    Heartbeat,       // 工作节点心跳。
    ClusterSnapshot, // 注册中心推送当前服务表快照。
    RaftAppend,      // Raft 日志复制请求。
    RaftVote,        // Raft 投票请求。
    Error,           // 协议错误响应。
}

impl ClusterFrameKind {
    // 返回 TCP 帧类型编码。
    pub(crate) fn code(self) -> u16 {
        match self {
            Self::RegisterService => 1,
            Self::Heartbeat => 2,
            Self::ClusterSnapshot => 3,
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
            3 => Some(Self::ClusterSnapshot),
            10 => Some(Self::RaftAppend),
            11 => Some(Self::RaftVote),
            u16::MAX => Some(Self::Error),
            _ => None,
        }
    }
}

// 集群 TCP 帧头。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterFrameHeader {
    pub kind: ClusterFrameKind, // TCP 帧类型。
    pub body_len: u32,          // TCP 帧 body 字节长度。
}
