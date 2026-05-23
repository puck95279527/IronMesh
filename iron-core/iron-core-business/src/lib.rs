// IronMesh 业务通信核心。
//
// 本 crate 存放业务侧核心模型，并导出业务 FlatBuffers 协议树。

// 业务通信数据模型模块。
pub mod model;

// 业务 FlatBuffers 协议导出模块。
pub mod scheme {
    // 导出业务协议生成类型。
    pub use iron_scheme_business::scheme::ironmesh::protocol::business::*;
}
