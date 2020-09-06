use tokio::net::TcpStream;
//type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
use anyhow::{ Result, Context };
use tokio::io::AsyncWriteExt;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncBufReadExt;
use core::task::Poll;
use std::pin::Pin;
use tokio::io::{ AsyncBufRead, AsyncRead, BufReader };
use std::marker::Unpin;
use tokio::net::tcp::{ OwnedWriteHalf, OwnedReadHalf };
use chrono::prelude::*;
use bytes::{ Bytes, BytesMut };
fn default<T: Default>() -> T { std::default::Default::default() } // default_free_fn
use tokio::sync::mpsc;
use std::sync::{ Arc, Mutex as SyncMutex };
use std::collections::HashMap;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone)]
pub struct RsyncClient {
    reqs: Requests,
    write: Arc<AsyncMutex<OwnedWriteHalf>>
}

impl RsyncClient {
    pub async fn connect(url: url::Url) -> Result<(RsyncClient, Vec<File>)> {
        let (path, base) = Self::parse_url(&url)?;
        let (read, mut write) = Self::stream(&url).await?;
        let mut read = BufReader::new(read);
        Self::send_handshake(&mut write, path, base).await?;
        Self::read_handshake(&mut read, base).await?;
        let mut read = EnvelopeRead::new(read); // Server multiplex start
        let files = Self::read_file_list(&mut read).await?;
        write.write_i32_le(-1).await?; anyhow::ensure!(read.read_i32_le().await? == -1, "Phase switch receive-list -> receive-file");
        let reqs: Requests = Arc::new(SyncMutex::new(Some(default())));
        tokio::spawn(ReadFilesProcess { read, reqs: reqs.clone() }.run());
        Ok((RsyncClient { reqs: reqs.clone(), write: Arc::new(AsyncMutex::new(write)) }, files))
    }
    pub async fn get(&mut self, file: &File) -> Result<impl AsyncRead> {
        // TODO: Somehow make sure that this is our file, with an index from our list - and not a different instance
        let (data_send, data_recv) = mpsc::channel(10);
        self.reqs.lock().unwrap()
            .as_mut()
            .context("Client fully exited")?
            .entry(file.idx)
            .or_insert(vec![])
            .push(data_send);
        let mut write = self.write.lock().await;
        write.write_i32_le(file.idx as i32).await?;
        write.write_all(&[0u8; 16]).await?; // We want no blocks, blocklength, checksum, or terminal block. 4 x 0i32 = 16
        Ok(tokio::io::stream_reader(data_recv))
    }


    fn parse_url(url: &url::Url) -> Result<(&Path, &str)> {
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
        Ok((path, base))
    }
    async fn stream(url: &url::Url) -> Result<(OwnedReadHalf, OwnedWriteHalf)> {
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
        Ok(stream.context(format!("Connect to {} ({:?})", url, addrs))?.into_split())
    }
    async fn send_handshake(write: &mut OwnedWriteHalf, path: &Path, base: &str) -> Result<()> {
        // rsync seems to be ok with us sending it all at once
        let initial: Vec<u8> = [
            // TODO: The openrsync docs only exist for rsync 27, but below rsync 30, dates beyond 1970 + 2^31 can't be handled.
            // Client hello
            &b"@RSYNCD: 27.0\n"[..],
            // Select root
            base.as_bytes(),
            &b"\n"[..],
            // Remote command
            &b"--server\n"[..],
            &b"--sender\n"[..],
            &b"-rl\n"[..],
            &b".\n"[..],
            path.to_str().unwrap().as_bytes(),
            &b"/\n\n"[..],
            // Exclusion list
            &b"\0\0\0\0"[..],
        ].concat();
        write.write_all(&initial).await?;
        Ok(())
    }
    async fn read_handshake<T: AsyncBufRead + Unpin>(read: &mut T, base: &str) -> Result<()> {
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
        Ok(())
    }
    async fn read_file_list<T: AsyncRead + Unpin + Send>(read: &mut T) -> Result<Vec<File>> {
        let mut ret = vec![];
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
            let show = String::from_utf8_lossy(&filename_buf);
            let size = read.read_rsync_long().await?;
            anyhow::ensure!(size >= 0, "Protocol error: negative file size for {}", show);
            let size = size as u64;
            mtime_buf = if meta.mtime_repeated() { mtime_buf } else {
                let ts = read.read_i32_le().await?;
                let naive = NaiveDateTime::from_timestamp(ts as i64, 0);
                let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
                Some(datetime)
            };
            let mode = if meta.file_mode_repeated() {
                mode_buf.context(format!("Protocol error: first file {} without mode", show))?
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
            let symlink = if unix_mode::is_symlink(mode) {
               let len = read.read_u32_le().await? as usize;
               let mut link = vec![];
               link.resize(len, 0);
               read.read_exact(&mut link).await?;
               Some(link)
            } else { None };
            let idx = usize::MAX;
            let f = File {
                path: filename_buf.clone(),
                mtime: mtime_buf,
                symlink, mode, size, idx,
            };
            println!("{}", f);
            ret.push(f);
        }
        let io_err = read.read_i32_le().await?;
        anyhow::ensure!(io_err == 0, "Protocol error: expected 0 IO errors, got {}.", io_err);
        ret.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        // Hmm. rsyn also dedupes. I've never seen dupes, so I don't know why.
        for (i, mut f) in ret.iter_mut().enumerate() {
            f.idx = i;
        }
        Ok(ret)
    }
}

type Requests = Arc<SyncMutex<Option<HashMap<usize, Vec<mpsc::Sender<Result<Bytes, tokio::io::Error>>>>>>>;

struct ReadFilesProcess {
    read: EnvelopeRead<BufReader<OwnedReadHalf>>,
    reqs: Requests,
}

impl ReadFilesProcess {
    async fn run(mut self) -> Result<()> {
        let res = self.process().await;
        println!("Client exiting");
        let remaining = self.reqs.lock().unwrap().take();
        for (_, cs) in remaining.unwrap().into_iter() {
            for mut c in cs.into_iter() {
                use tokio::io::{ Error, ErrorKind };
                c.send(Err(Error::new(ErrorKind::ConnectionAborted, anyhow::anyhow!("Client exited")))).await.ok();
            }
        }
        res
    }
    async fn process(&mut self) -> Result<()> {
        loop {
            let idx = self.read.read_i32_le().await?;
            if idx == -1 {
                break Ok(());
            }
            { // block info
                let mut buf = [0u8; 16];
                self.read.read_exact(&mut buf).await?;
                anyhow::ensure!(buf == [0u8; 16], "Protocol error: we requested a plain file, not blocks and checksums");
            }
            let idx = idx as usize;
            let mut backchan = {
                let mut table = self.reqs.lock().unwrap();
                let table = table.as_mut().unwrap();
                let mut reqs = table.get_mut(&idx);
                let req = reqs.as_mut().and_then(|v| v.pop()).context("Got file - no memory of requesting it")?;
                if reqs.map(|v| v.len()) == Some(0) { table.remove(&idx).unwrap(); }
                req
            };
            loop {
                let chunklen = self.read.read_i32_le().await?;
                if chunklen == 0 {
                    break;
                }
                anyhow::ensure!(chunklen > 0, "Protocol error: negative sized chunk");
                let mut chunklen = chunklen as usize;
                // TODO: send to current chan
                while chunklen > 0 {
                    let read = std::cmp::min(1 << 16, chunklen);
                    let mut buf = BytesMut::with_capacity(read);
                    chunklen -= self.read.read_buf(&mut buf).await?;
                    backchan.send(Ok(buf.into())).await?;
                }
            }
            { // Hash. TODO: check
                let mut buf = [0u8; 16];
                self.read.read_exact(&mut buf).await?;
            }
        }
    }
}

pub struct File {
    pub path: Vec<u8>,
    pub symlink: Option<Vec<u8>>,
    /// e.g. unix_mode is useful for parsing
    pub mode: u32,
    pub size: u64,
    pub mtime: Option<DateTime<Utc>>,
    idx: usize,
}
impl std::fmt::Display for File {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "{} {:>12} {} {}{}",
            unix_mode::to_string(self.mode),
            self.size,
            self.mtime.as_ref().map(DateTime::to_rfc3339).unwrap_or("                    ".to_owned()),
            String::from_utf8_lossy(&self.path),
            self.symlink.as_ref().map(|s| format!(" -> {}", String::from_utf8_lossy(&s))).unwrap_or("".to_owned())
        )?;
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


#[cfg(test)]
mod tests {
    use super::*;
 
    #[tokio::test]
    async fn it_works() {
        let url = url::Url::parse("rsync://cameo/ftp").unwrap();
        let (mut cli, files) = RsyncClient::connect(url).await.unwrap();
        let mut req = BufReader::new(cli.get(&files[4]).await.unwrap());
        let mut buf = vec![];
        req.read_to_end(&mut buf).await.unwrap();
        hexdump::hexdump(&buf);
        let mut req = BufReader::new(cli.get(&files[1]).await.unwrap());
        let mut buf = vec![];
        req.read_to_end(&mut buf).await.unwrap();
        hexdump::hexdump(&buf);
    }
}
