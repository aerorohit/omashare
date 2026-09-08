use anyhow::{Context, Result};
use bytes::BytesMut;
use tokio::io::AsyncWriteExt;

use crate::protocol::WireMsg;

pub async fn write_msg<S>(send: &mut S, msg: &WireMsg) -> Result<()>
where
    S: quinn_write::WriteBytes,
{
    let bytes = serde_json::to_vec(msg)?;
    anyhow::ensure!(bytes.len() <= 8 * 1024 * 1024, "message too large");
    send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    send.write_all(&bytes).await?;
    Ok(())
}

pub async fn read_msg<R>(recv: &mut R) -> Result<WireMsg>
where
    R: quinn_write::ReadBytes,
{
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await.context("read length")?;
    let len = u32::from_be_bytes(len_buf) as usize;
    anyhow::ensure!(len > 0 && len <= 8 * 1024 * 1024, "invalid length {len}");
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await.context("read body")?;
    Ok(serde_json::from_slice(&buf)?)
}

/// Thin adapters so we can compile against iroh's quinn streams without tokio Async*.
pub mod quinn_write {
    use anyhow::Result;
    use iroh::endpoint::{RecvStream, SendStream};

    pub trait WriteBytes {
        fn write_all(&mut self, buf: &[u8])
            -> impl std::future::Future<Output = Result<()>> + Send;
    }

    pub trait ReadBytes {
        fn read_exact(
            &mut self,
            buf: &mut [u8],
        ) -> impl std::future::Future<Output = Result<()>> + Send;
    }

    impl WriteBytes for SendStream {
        async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
            SendStream::write_all(self, buf).await?;
            Ok(())
        }
    }

    impl ReadBytes for RecvStream {
        async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
            let mut off = 0;
            while off < buf.len() {
                match RecvStream::read(self, &mut buf[off..]).await? {
                    Some(n) => off += n,
                    None => anyhow::bail!("unexpected end of stream"),
                }
            }
            Ok(())
        }
    }
}

// silence unused in some iroh versions
#[allow(dead_code)]
pub fn _unused(_b: &BytesMut) {}

#[allow(dead_code)]
pub async fn _flush<S: AsyncWriteExt + Unpin>(s: &mut S) -> Result<()> {
    s.flush().await?;
    Ok(())
}
