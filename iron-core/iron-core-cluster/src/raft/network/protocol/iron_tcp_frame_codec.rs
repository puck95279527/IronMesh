use std::io;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use crate::raft::network::protocol::IronTcpRequest;
use crate::raft::network::protocol::IronTcpResponse;

// IronMesh Raft TCP 数据帧编解码器。
#[derive(Clone, Debug, Default)]
pub struct IronTcpFrameCodec;

impl IronTcpFrameCodec {
    // 从 JSON 字节解码 TCP 请求。
    pub fn decode_request(frame: Bytes) -> Result<IronTcpRequest, io::Error> {
        serde_json::from_slice::<IronTcpRequest>(&frame)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    // 把 TCP 请求编码成 JSON 字节。
    pub fn encode_request(request: &IronTcpRequest) -> Result<Bytes, io::Error> {
        serde_json::to_vec(request)
            .map(Bytes::from)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    // 从 JSON 字节解码 TCP 响应。
    pub fn decode_response(frame: Bytes) -> Result<IronTcpResponse, io::Error> {
        serde_json::from_slice::<IronTcpResponse>(&frame)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    // 把 TCP 响应编码成 JSON 字节。
    pub fn encode_response(response: &IronTcpResponse) -> Result<Bytes, io::Error> {
        serde_json::to_vec(response)
            .map(Bytes::from)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

impl Encoder<Bytes> for IronTcpFrameCodec {
    type Error = io::Error;

    // 编码一帧 TCP 字节数据。
    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = item.len() as u32;
        dst.put_u32(len);
        dst.extend_from_slice(&item);
        Ok(())
    }
}

impl Decoder for IronTcpFrameCodec {
    type Item = Bytes;
    type Error = io::Error;

    // 解码一帧 TCP 字节数据。
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = (&src[..4]).get_u32() as usize;
        if src.len() < 4 + len {
            return Ok(None);
        }

        src.advance(4);
        let frame = src.split_to(len);
        Ok(Some(frame.freeze()))
    }
}
