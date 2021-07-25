use crate::alluse::*;
use tokio::io::ReadBuf;

/// Strips rsync data frame headers and prints non-data frames as warning messages
pub struct EnvelopeRead<T: tokio::io::AsyncBufRead + Unpin> {
	// TODO: rebuild with an enum of pending body, pending error, and pending header
	read: T,
	frame_remaining: usize,
	pending_error: Option<(u8, Vec<u8>)>,
	pending_header: Option<([u8; 4], u8)>,
}

impl<T: tokio::io::AsyncBufRead + Unpin> EnvelopeRead<T> {
	pub fn new(t: T) -> EnvelopeRead<T> {
		EnvelopeRead {
			read: t,
			frame_remaining: 0,
			pending_error: None,
			pending_header: None,
		}
	}

	fn poll_err(
		mut self: Pin<&mut Self>,
		ctx: &mut std::task::Context<'_>,
		repoll_buf: &mut ReadBuf,
	) -> Poll<Result<(), std::io::Error>> {
		while self.frame_remaining > 0 {
			let mut pe = self
				.pending_error
				.as_ref()
				.expect("Error expected, but not present")
				.1
				.clone();
			let pei = pe.len() - self.frame_remaining; // Ich check dich wek alter bowwochecka
			let rb = &mut ReadBuf::new(&mut pe[pei..]);
			match Pin::new(&mut self.read).poll_read(ctx, rb) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
				Poll::Ready(Ok(())) => {
					if rb.filled().len() == 0 {
						break;
					}
					self.frame_remaining -= rb.filled().len();
					self.pending_error.as_mut().unwrap().1 = pe;
				}
			};
		}
		let (typ, msg) = self.pending_error.take().unwrap();
		let msg = String::from_utf8_lossy(&msg);
		let msg = msg
			.strip_suffix("\n")
			.filter(|msg| msg.matches("\n").count() == 0)
			.unwrap_or(&msg)
			.to_owned();
		let e = match typ {
			8 => {
				log::warn!("Sender: {}", msg);
				return self.poll_read(ctx, repoll_buf);
			}
			1 => anyhow::anyhow!("Server error: {}", msg),
			t => anyhow::anyhow!("Unknown error {}: {}", t, msg),
		};
		return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, e)));
	}
}

impl<T: tokio::io::AsyncBufRead + Unpin> AsyncRead for EnvelopeRead<T> {
	fn poll_read(
		mut self: Pin<&mut Self>,
		ctx: &mut std::task::Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<Result<(), std::io::Error>> {
		if self.pending_error.is_some() {
			return self.poll_err(ctx, buf);
		}
		while self.frame_remaining == 0 {
			let no_pending_header = self.pending_header.is_none();
			let pll = Pin::new(&mut self.read).poll_fill_buf(ctx);
			match pll {
				// Starting to wonder whether it wouldn't be easier to store the future returend by read_u32_le
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
				Poll::Ready(Ok([])) if no_pending_header => return Poll::Ready(Ok(())),
				Poll::Ready(Ok([])) => {
					return Poll::Ready(Err(std::io::Error::new(
						std::io::ErrorKind::ConnectionAborted,
						anyhow::anyhow!("Abort during header read"),
					)))
				}
				Poll::Ready(Ok(slice)) => {
					let slicehead = &mut [0u8; 4];
					let consumable = std::cmp::min(slice.len(), 4);
					slicehead[..consumable].copy_from_slice(&slice[..consumable]);
					let (mut ph, phl) = self.pending_header.take().unwrap_or(([0u8; 4], 0));
					let phl = phl as usize;
					let consumable = std::cmp::min(ph.len() - phl, consumable);
					ph[phl..(phl + consumable)].copy_from_slice(&slicehead[..consumable]);
					let phl = phl + consumable;
					Pin::new(&mut self.read).consume(consumable);
					if phl < ph.len() {
						self.pending_header = Some((ph, phl as u8));
					} else {
						let [b1, b2, b3, b4] = ph;
						let b1 = b1 as usize;
						let b2 = b2 as usize;
						let b3 = b3 as usize;
						self.frame_remaining = b1 + b2 * 0x100 + b3 * 0x100_00 as usize;
						log::trace!("Frame {} {}", b4, self.frame_remaining);
						match b4 {
							7 => (),
							t => {
								let mut errbuf = vec![];
								errbuf.resize(self.frame_remaining, 0);
								self.pending_error = Some((t, errbuf));
								return self.poll_err(ctx, buf);
							}
						};
					}
				}
			}
		}
		let request = std::cmp::min(buf.capacity(), self.frame_remaining as usize);
		let mut rb = buf.take(request);
		match Pin::new(&mut self.read).poll_read(ctx, &mut rb) {
			p @ Poll::Pending => p,
			e @ Poll::Ready(Err(_)) => e,
			r @ Poll::Ready(Ok(())) => {
				let read = rb.filled().len();
				if log::log_enabled!(log::Level::Trace) {
					log::trace!(
						"Read {} / {}:\n{}",
						read,
						self.frame_remaining,
						hexdump::hexdump_iter(rb.filled())
							.map(|l| l.to_string())
							.collect::<Vec<_>>()
							.join("\n")
					);
				}
				std::mem::drop(rb);
				self.frame_remaining -= read;
				buf.advance(read);
				r
			}
		}
	}
}

#[async_trait::async_trait]
pub trait RsyncReadExt: AsyncRead + Unpin {
	/// For reading rsync's variable length integers… quite an odd format.
	async fn read_rsync_long(&mut self) -> Result<i64> {
		let v = self.read_i32_le().await?;
		Ok(if v == -1 { self.read_i64_le().await? } else { v as i64 })
	}
}
impl<T: tokio::io::AsyncRead + Unpin> RsyncReadExt for T {}
