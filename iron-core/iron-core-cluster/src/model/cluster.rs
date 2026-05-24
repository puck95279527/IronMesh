// 集群状态数据结构。

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// Raft 服务角色。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RaftServiceRole {
    Leader,    // Raft Leader，负责接收写请求、追加日志，并把日志复制给其他 Raft 节点。
    Follower,  // Raft Follower，有投票权，接收 Leader 的日志复制，并在选举时参与投票。
    Candidate, // Raft Candidate，有投票权，表示节点正在发起选举并向其他节点请求投票。
    Learner,   // Raft Learner，没有投票权，只接收日志复制，通常用于新节点追赶数据或扩容过渡。
}

// 业务服务类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum BizServiceKind {
    Registry, // 注册表业务服务。
    Gate,     // 网关业务服务。
    Auth,     // 登录注册业务服务。
    GamePdk,  // 跑得快业务服务。
    GameDdz,  // 斗地主业务服务。
}

impl BizServiceKind {
    // 返回默认业务服务实例 ID。
    pub fn default_biz_service_id(self) -> &'static str {
        match self {
            Self::Registry => "registry-1",
            Self::Gate => "gate-1",
            Self::Auth => "auth-1",
            Self::GamePdk => "game_pdk-1001",
            Self::GameDdz => "game_ddz-1001",
        }
    }
}

// 业务端点。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BizService {
    pub name: String, // 业务端点名称，例如 ctrl / data / http / ws / admin。
    pub addr: String, // 业务端点地址，例如 10.0.0.8:8888。
}

// Raft 与业务服务实例。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronClusterService {
    pub raft_id: Option<u64>, // Raft 服务实例 ID，只有 registry Raft 节点有值。
    pub raft_role: Option<RaftServiceRole>, // Raft 当前角色，worker 服务为 None。
    pub raft_addr: Option<String>, // Raft 通信地址，worker 服务为 None。
    pub raft_epoch: Option<u64>, // Raft 实例启动代次，worker 服务为 None。
    pub raft_alive_at_ms: Option<u64>, // Raft 最近心跳时间，worker 服务为 None。
    pub biz_kind: BizServiceKind, // 业务服务类型。
    pub biz_service_id: String, // 业务服务实例 ID，例如 game_pdk-1001。
    pub biz_services: Vec<BizService>, // 当前实例暴露的业务端点列表。
}

// 集群状态写命令。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClusterCommand {
    Upsert(IronClusterService), // 注册或更新服务。
    Offline {
        biz_service_id: String, // 标记下线的业务服务实例 ID。
    },
}

impl ClusterCommand {
    // 应用命令到集群状态表。
    pub(crate) fn apply_to(self, data: &mut BTreeMap<String, IronClusterService>) {
        match self {
            Self::Upsert(service) => {
                data.insert(service.biz_service_id.clone(), service);
            }
            Self::Offline { biz_service_id } => {
                data.remove(&biz_service_id);
            }
        }
    }
}
