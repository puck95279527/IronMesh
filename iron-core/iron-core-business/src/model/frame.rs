// 业务传输帧模型定义。
//
// 本模块只定义数据模型，不提供字节编解码、网络 I/O、路由或校验逻辑。

// 固定业务帧头长度，单位为字节。
pub const IRON_BUSINESS_FRAME_HEADER_LEN: usize = 24;

// 序列化后的业务用户 ID 长度，单位为字节。
pub const IRON_BUSINESS_USER_ID_LEN: usize = 8;

// 网关和业务服务共同使用的用户 ID 类型。
pub type IronBusinessTargetUserId = u64;

// 业务帧语义类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum IronBusinessFrameKind {
    None = 0,      // 未指定业务帧类型。
    Request = 1,   // 客户端或内部服务发起请求。
    Response = 2,  // 服务端返回请求响应。
    Broadcast = 3, // 服务端主动广播消息。
}

// 业务广播路由范围。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum IronBusinessBroadcastScope {
    None = 0,  // 非广播帧或未指定广播范围。
    All = 1,   // 广播给全部目标用户。
    Users = 2, // 广播给指定目标用户列表。
}

// 固定 24 字节业务帧头语义模型。
//
// 线上的字节表示使用小端序，逻辑布局为：
//
// ```text
// body_len        u32
// kind            u8
// broadcast_scope u8
// target_count    u16
// request_id      u64
// actor_user_id   u64
// ```
//
// 这个类型不是线上的内存布局类型，不要 transmute，也不要依赖 Rust 内存布局做协议编码。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IronBusinessFrameHeader {
    pub body_len: u32,                               // 业务体字节长度。
    pub kind: IronBusinessFrameKind,                 // 业务帧语义类型。
    pub broadcast_scope: IronBusinessBroadcastScope, // 广播路由范围。
    pub target_count: u16,                           // 指定目标用户数量。
    pub request_id: u64,                             // 请求与响应关联 ID。
    pub actor_user_id: u64,                          // 可信调用方用户 ID。
}

// 业务帧分段模型。
//
// 普通请求、响应、全员广播帧：
//
// ```text
// [IronBusinessFrameHeader][FbsBody]
// ```
//
// 指定用户广播帧：
//
// ```text
// [IronBusinessFrameHeader][IronBusinessTargetUserId...][FbsBody]
// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IronBusinessFrame<TBody> {
    pub header: IronBusinessFrameHeader,                // 业务帧头。
    pub target_user_ids: Vec<IronBusinessTargetUserId>, // 指定广播目标用户列表。
    pub body: TBody,                                    // FlatBuffers 业务体或调用方指定的业务体。
}
