use anyhow::Result;
use bytes::Bytes;
use std::io::{self, ErrorKind, Write};
use tokio::sync::mpsc::error::TrySendError;
// use std::sync::mpsc::Sender;
use std::time::Instant;
use tokio::sync::mpsc::Sender;

/// Writer that pipes data through MPSC channel
pub struct ChannelWriter {
    sender: Sender<Result<Bytes>>,
    // buffer: BytesMut,
    // buffer_limit: usize,
    total_bytes_written: u64,
    last_flush_time: Instant,
}

impl ChannelWriter {
    /// Create a new ChannelWriter with the given channel sender
    pub fn new(sender: Sender<Result<Bytes>>) -> Self {
        Self {
            sender,
            // buffer: BytesMut::new(),
            // buffer_limit: 1024 * 1024, // 1MB default buffer
            total_bytes_written: 0,
            last_flush_time: Instant::now(),
        }
    }

    // Get total bytes written
    pub fn total_bytes_written(&self) -> u64 {
        self.total_bytes_written
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if let Err(e) = self.sender.try_send(Ok(Bytes::copy_from_slice(buf))) {
            match e {
                TrySendError::Closed(_) => {
                    return Err(io::Error::new(ErrorKind::Other, e.to_string()));
                }
                _ => {
                    return Ok(0);
                }
            }
        }

        self.total_bytes_written += buf.len() as u64;

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        return Ok(());
    }
}

// Async version for use with async/await
// pub struct AsyncChannelWriter {
//     sender: Sender<Bytes>,
//     buffer: BytesMut,
//     buffer_limit: usize,
// }

// impl AsyncChannelWriter {
//     pub fn new(sender: Sender<Bytes>) -> Self {
//         Self {
//             sender,
//             buffer: BytesMut::new(),
//             buffer_limit: 1024 * 1024,
//         }
//     }

//     /// Async write - non-blocking send
//     pub async fn write(&mut self, data: &[u8]) -> io::Result<usize> {
//         if self.buffer.len() + data.len() > self.buffer_limit {
//             self.flush().await?;
//         }

//         self.buffer.extend_from_slice(data);

//         if self.buffer.len() >= self.buffer_limit {
//             self.flush().await?;
//         }

//         Ok(data.len())
//     }

//     /// Async flush
//     pub async fn flush(&mut self) -> io::Result<()> {
//         if self.buffer.is_empty() {
//             return Ok(());
//         }

//         let data = self.buffer.split().freeze();

//         // In async context, we might want to spawn blocking task
//         match self.sender.try_send(data) {
//             Ok(_) => Ok(()),
//             Err(TrySendError::Full(bytes)) => {
//                 self.buffer.extend_from_slice(&bytes);
//                 Err(Error::new(ErrorKind::WouldBlock, "Channel full"))
//             }
//             Err(TrySendError::Disconnected(_)) => {
//                 Err(Error::new(ErrorKind::BrokenPipe, "Channel disconnected"))
//             }
//         }
//     }

//     /// Write entire Bytes without copying
//     pub async fn write_bytes(&mut self, bytes: Bytes) -> io::Result<()> {
//         match self.sender.try_send(bytes) {
//             Ok(_) => Ok(()),
//             Err(TrySendError::Full(bytes)) => {
//                 self.buffer.extend_from_slice(&bytes);
//                 if self.buffer.len() >= self.buffer_limit {
//                     self.flush().await?;
//                 }
//                 Ok(())
//             }
//             Err(TrySendError::Disconnected(_)) => {
//                 Err(Error::new(ErrorKind::BrokenPipe, "Channel disconnected"))
//             }
//         }
//     }
// }
