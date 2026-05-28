use std::io;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

// IronMesh Raft TCP 数据帧编解码器。
#[derive(Clone, Debug, Default)]
pub struct IronTcpFrameCodec;

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
