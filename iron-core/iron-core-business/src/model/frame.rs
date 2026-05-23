//! Business transport frame model definitions.
//!
//! This module intentionally defines only the data model. It does not provide
//! byte encoding, byte decoding, network I/O, routing, or validation logic.

/// Fixed business frame header length in bytes.
pub const IRON_BUSINESS_FRAME_HEADER_LEN: usize = 24;

/// Serialized business user id length in bytes.
pub const IRON_BUSINESS_USER_ID_LEN: usize = 8;

/// User id used by the gateway and business services.
pub type IronBusinessTargetUserId = u64;

/// Business frame semantic type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum IronBusinessFrameKind {
    None = 0,
    Request = 1,
    Response = 2,
    Broadcast = 3,
}

/// Business broadcast routing scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum IronBusinessBroadcastScope {
    None = 0,
    All = 1,
    Users = 2,
}

/// Fixed 24-byte business frame header semantic model.
///
/// The wire representation is little-endian and has this logical layout:
///
/// ```text
/// body_len        u32
/// kind            u8
/// broadcast_scope u8
/// target_count    u16
/// request_id      u64
/// actor_user_id   u64
/// ```
///
/// This type is not a wire layout type. Do not transmute it or rely on its
/// Rust memory layout for protocol encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IronBusinessFrameHeader {
    pub body_len: u32,
    pub kind: IronBusinessFrameKind,
    pub broadcast_scope: IronBusinessBroadcastScope,
    pub target_count: u16,
    pub request_id: u64,
    pub actor_user_id: u64,
}

/// Business frame segment model.
///
/// Normal request, response, and broadcast-to-all frames are:
///
/// ```text
/// [IronBusinessFrameHeader][FbsBody]
/// ```
///
/// Broadcast-to-users frames are:
///
/// ```text
/// [IronBusinessFrameHeader][IronBusinessTargetUserId...][FbsBody]
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IronBusinessFrame<TBody> {
    pub header: IronBusinessFrameHeader,
    pub target_user_ids: Vec<IronBusinessTargetUserId>,
    pub body: TBody,
}
