use tokio::net::TcpStream;
//type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
use anyhow::{ Result, Context };
use tokio::io::AsyncWriteExt;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncBufReadExt;
use core::task::Poll;
use std::pin::Pin;
use tokio::io::{ AsyncRead, AsyncWrite, BufReader };
use std::marker::Unpin;
use tokio::net::tcp::{ OwnedWriteHalf, OwnedReadHalf };

struct RsyncClient {
    read: EnvelopeRead<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
    files: (),
}
impl RsyncClient {
    pub async fn new(url: url::Url) -> Result<RsyncClient> {
        anyhow::ensure!(url.scheme() == "rsync", "Only rsync urls supported, not {}", url);
        anyhow::ensure!(url.path() != "", "Path cannot be / - your url {} is cut short.", url);
        let path = Path::new(url.path());
        let path = if path.is_absolute() {
           path.strip_prefix(Path::new("/")).expect(&format!("Stripping root from path {:?}", path))
           // Does this work on windows?
        } else { path };
        let mut base = path;
        let base = loop {
            match base.parent() {
                Some(p) if p == Path::new("") => break base,
                None => panic!("Getting base element of path {:?}", ),
                Some(p) => base = p,
            };
        };
        let base = base.to_str().expect("TODO: handle paths properly");
        let mut addrs = url.socket_addrs(|| Some(873))
            .context(format!("Get socket addr from url {}", url))?;
        let mut stream = TcpStream::connect(
            addrs.pop().expect("Successful resolution returns at least one address")
        ).await;
        for addr in addrs.iter() {
            stream = TcpStream::connect(addr).await;
            if stream.is_ok() {
                break;
            }
        }
        let (read, mut write) = stream.context(format!("Connect to {} ({:?})", url, addrs))?.into_split();
        // TODO: The openrsync docs only exist for rsync 27, but below rsync 30, dates beyond 1970 + 2^31 can't be handled.
        let initial: Vec<u8> = [
            &b"@RSYNCD: 27.0\n"[..],
            base.as_bytes(),
            &b"\n"[..],
            &b"--server\n"[..],
            &b"--sender\n"[..],
            &b"-r\n"[..],
            &b".\n"[..],
            path.to_str().unwrap().as_bytes(),
            &b"/\n\n"[..],
            &b"\0\0\0\0"[..],
        ].concat();
        write.write_all(&initial).await?;
        //stream.shutdown(std::net::Shutdown::Write)?;
        let mut read = BufReader::new(read);
        let hello = &mut String::new();
        read.read_line(hello)
            .await.context("Read server hello")?;
        let rsyncd = "@RSYNCD: ";
        anyhow::ensure!(hello.starts_with(rsyncd), "Not an rsync server? First message was {}", hello);
        let ver = hello[rsyncd.len() .. (hello.len() - 1)]
            .split('.').map(str::parse)
            .collect::<Result<Vec<u64>, _>>()
            .context(format!("Parsing server version from {}", hello))?;
        anyhow::ensure!(ver >= vec![27, 0], "Server version {} not supported - need 27.0 minimum.",
            ver.iter().map(|i| format!("{}", i)).collect::<Vec<_>>().join(".")
        );
        let select = &mut String::new();
        read.read_line(select)
            .await.context("Read server startup")?;
        anyhow::ensure!(select == &format!("{}OK\n", rsyncd), "Could not select {}: {}", base, select);
        let _random_seed = read.read_u32().await?; // Read random seed - we don't care
        // Server multiplex start
        let mut read = EnvelopeRead::new(read);
        let mut filename_buf: Vec<u8> = vec![];
        let mut mode_buf = None;
        let mut mtime_buf = None;
        loop {
            let meta = FileEntryStatus(read.read_u8().await?);
            if meta.is_end() {
                break;
            }
            let inherited_filename_length = if meta.inherits_filename() { read.read_u8().await? as usize } else { 0 };
            anyhow::ensure!(inherited_filename_length <= filename_buf.len(), "Protocol error: file list format inconsistency");
            let filename_length = if meta.integer_filename_len() {
                read.read_u32_le().await? as usize 
            } else {
                read.read_u8().await? as usize
            };
            filename_buf.resize(filename_length + inherited_filename_length, 0);
            read.read_exact(&mut filename_buf[inherited_filename_length .. ]).await?;
            let size = read.read_rsync_long().await?;
            mtime_buf = if meta.mtime_repeated() { mtime_buf } else { Some(read.read_i32_le().await?) };
            let mode = if meta.file_mode_repeated() {
                mode_buf.context(format!("Protocol error: first file {} without mode", String::from_utf8_lossy(&filename_buf)))?
            } else {
                read.read_u32_le().await?
            };
            mode_buf = Some(mode);
            if !meta.uid_repeated() {
                // Actually, I expect the uids to be never present. Oh well.
                let _uid = read.read_i32_le().await?;
            }
            if !meta.gid_repeated() {
                let _gid = read.read_i32_le().await?;
            }
            // Maydo: skip rdev and symlink targets (shouldn't be present since we don't pass -Dl)
            println!("{:0>16b} {} {:?} {}", mode, size, mtime_buf, String::from_utf8_lossy(&filename_buf));
        }
        anyhow::ensure!(read.read_i32_le().await? == 0, "Protocol error: expected 0"); // What it is good for? No idea.
        let mut client = RsyncClient { read, write, files: () };
        client.phase_switch().await?;
        Ok(client)
    }
    async fn phase_switch(&mut self) -> Result<()> {
        self.write.write_i32_le(-1i32).await?;
        anyhow::ensure!(self.read.read_i32_le().await? == -1, "Protocol error: phase switch"); // What it is good for? No idea.
        Ok(())
    }
    #[allow(dead_code)]
    async fn dbg_close(mut self) -> Result<()> {
        std::mem::drop(self.write);
        let buf = &mut vec![];
        self.read.read_to_end(buf).await?;
        hexdump::hexdump(buf);
        Ok(())
    }
}

struct EnvelopeRead<T: tokio::io::AsyncBufRead + Unpin> {
    read: T,
    frame_remaining: u32,
    pending_error: Option<String>,
}

impl<T: tokio::io::AsyncBufRead + Unpin> EnvelopeRead<T> {
    fn new(t: T) -> EnvelopeRead<T> { EnvelopeRead { read: t, frame_remaining: 0, pending_error: None } }

    fn poll_err(mut self: Pin<&mut Self>, ctx: &mut std::task::Context<'_>)
        -> Poll<Result<usize, std::io::Error>>
    {
        let mut pe = self.pending_error.clone().expect("Error expected, but not present");
        while self.frame_remaining > 0 && pe.len() < 1 << 14 {
            let buf = &mut [0u8; 256];
            match Pin::new(&mut self.read).poll_read(ctx, buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(0)) => break,
                Poll::Ready(Ok(r)) => {
                    pe += &String::from_utf8_lossy(&buf[..r]);
                    self.pending_error = Some(pe.clone());
                },
            };
        }
        let e = anyhow::anyhow!("{}", pe);
        return Poll::Ready(Err(
            std::io::Error::new(std::io::ErrorKind::ConnectionAborted, e)
        ));
    }
}

impl<T: tokio::io::AsyncBufRead + Unpin> AsyncRead for EnvelopeRead<T> {
    fn poll_read(mut self: Pin<&mut Self>, ctx: &mut std::task::Context<'_>, buf: &mut [u8])
        -> Poll<Result<usize, std::io::Error>>
    {
        if self.pending_error.is_some() {
            return self.poll_err(ctx);
        }
        if self.frame_remaining == 0 {
            loop {
                let pll = Pin::new(&mut self.read).poll_fill_buf(ctx);
                match pll {
                    // Starting to wonder whether it wouldn't be easier to store the future
                    // returend from read_u32_le
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok([])) => return Poll::Ready(Ok(0)),
                    Poll::Ready(Ok([b1, b2, b3, b4, ..])) => {
                        let b1 = *b1 as u32; let b2 = *b2 as u32; let b3 = *b3 as u32; let b4 = *b4 as u32;
                        Pin::new(&mut self.read).consume(4);
                        self.frame_remaining = b1 + b2 * 0x100 + b3 * 0x100_00;
                        println!("Frame {}", self.frame_remaining);
                        match b4 {
                            7 => (),
                            1 => {
                                self.pending_error = Some(format!("Server error: "));
                                return self.poll_err(ctx);
                            },
                            t => {
                                self.pending_error = Some(format!("Unknown frame type {}: ", t));
                                return self.poll_err(ctx);
                            }
                        };
                        break;
                    },
                    Poll::Ready(Ok(_)) => continue,
                }
            }
        }
        let request = std::cmp::min(buf.len(), self.frame_remaining as usize);
        Pin::new(&mut self.read).poll_read(ctx, &mut buf[0..request])
            .map(|r| r.map(|r| { self.frame_remaining -= r as u32; r }))
    }
}

struct FileEntryStatus(u8);
impl FileEntryStatus {
    fn is_end(&self) -> bool { self.0 == 0 }
    #[allow(dead_code)] // I have really no idea what it is good for
    fn is_top_level(&self) -> bool { self.0 & 0x01 != 0 }
    fn file_mode_repeated(&self) -> bool { self.0 & 0x02 != 0 }
    fn uid_repeated(&self) -> bool { self.0 & 0x08 != 0 }
    fn gid_repeated(&self) -> bool { self.0 & 0x10 != 0 }
    fn inherits_filename(&self) -> bool { self.0 & 0x20 != 0 }
    fn integer_filename_len(&self) -> bool { self.0 & 0x40 != 0 }
    fn mtime_repeated(&self) -> bool { self.0 & 0x80 != 0 }
}

#[async_trait::async_trait] 
trait RsyncReadExt: AsyncRead + Unpin {
    async fn read_rsync_long(&mut self) -> Result<i64> {
        let v = self.read_i32_le().await?;
        Ok(if v == -1 {
            self.read_i64_le().await?
        } else {
            v as i64
        })
    }
}

impl<T: tokio::io::AsyncRead + Unpin> RsyncReadExt for T {}

//impl<T: tokio::io::AsyncBufRead> tokio::io::AsyncBufRead for EnvelopeRead<T> {
//
//}


#[cfg(test)]
mod tests {
    use super::*;
 
    #[tokio::test]
    async fn it_works() {
        let url = url::Url::parse("rsync://cameo/ftp").unwrap();
        let mut cli = RsyncClient::new(url).await.unwrap();
        cli.phase_switch().await.unwrap();
        cli.dbg_close().await.unwrap();
    }
}
