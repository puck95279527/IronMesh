// IronMesh 集群通信核心。
//
// 本 crate 存放集群侧核心模型、服务注册发现能力，并导出集群 FlatBuffers 协议类型。

pub mod api;
mod config;
mod http;
pub mod model;
mod raft;
mod runtime;
mod tcp;

// 集群 FlatBuffers 协议导出模块。
pub mod scheme {
    // 导出集群协议生成类型。
    pub use iron_scheme_cluster::*;
}

pub use api::*;
pub use model::*;
