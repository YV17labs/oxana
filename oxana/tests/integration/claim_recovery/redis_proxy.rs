use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use deadpool_redis::redis::{self, ConnectionAddr};
use testresult::TestResult;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

/// Forwards RESP2 traffic to real Redis, then fails one matching response.
/// In particular, a failed LMOVE response happens *after* Redis commits it.
pub struct RedisProxy {
    pub pool: deadpool_redis::Pool,
    pub injected: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl RedisProxy {
    pub async fn start(command_prefix: Vec<String>, disconnect: bool) -> TestResult<Self> {
        let client = redis::Client::open(std::env::var("REDIS_URL")?)?;
        let info = client.get_connection_info().clone();
        let ConnectionAddr::Tcp(host, port) = info.addr().clone() else {
            return Err("Redis fault tests require a TCP Redis URL".into());
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let config = deadpool_redis::Config::from_connection_info(info.set_addr(
            ConnectionAddr::Tcp(address.ip().to_string(), address.port()),
        ));
        let pool = config.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        let injected = Arc::new(AtomicBool::new(false));
        let fault = Arc::clone(&injected);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let host = host.clone();
                        let prefix = command_prefix.clone();
                        let fault = Arc::clone(&fault);
                        connections.spawn(async move {
                            let result = forward(socket, &host, port, &prefix, disconnect, fault).await;
                            if let Err(error) = result {
                                assert!(matches!(error.kind(), io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe), "{error}");
                            }
                        });
                    }
                    Some(result) = connections.join_next() => result.unwrap(),
                }
            }
        });
        Ok(Self {
            pool,
            injected,
            task,
        })
    }
}

impl Drop for RedisProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn forward(
    socket: TcpStream,
    host: &str,
    port: u16,
    prefix: &[String],
    disconnect: bool,
    injected: Arc<AtomicBool>,
) -> io::Result<()> {
    let upstream = TcpStream::connect((host, port)).await?;
    upstream.set_nodelay(true)?;
    socket.set_nodelay(true)?;
    let mut upstream = BufReader::new(upstream);
    let mut socket = BufReader::new(socket);
    loop {
        let request = read_frame(&mut socket).await?;
        upstream.get_mut().write_all(&request).await?;
        let response = read_frame(&mut upstream).await?;
        let command: Vec<String> =
            redis::from_redis_value(redis::parse_redis_value(&request).map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
        if command.starts_with(prefix)
            && response != b"$-1\r\n"
            && !injected.swap(true, Ordering::SeqCst)
        {
            if disconnect {
                return Ok(());
            }
            socket
                .get_mut()
                .write_all(b"-ERR injected claim failure\r\n")
                .await?;
        } else {
            socket.get_mut().write_all(&response).await?;
        }
    }
}

async fn read_frame(stream: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    let mut frame = Vec::new();
    let mut remaining = 1;
    while remaining > 0 {
        let start = frame.len();
        if stream.read_until(b'\n', &mut frame).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        remaining -= 1;
        match frame[start] {
            b'*' | b'$' => {
                let length: i64 = std::str::from_utf8(&frame[start + 1..frame.len() - 2])
                    .map_err(io::Error::other)?
                    .parse()
                    .map_err(io::Error::other)?;
                if frame[start] == b'*' {
                    remaining += length.max(0);
                } else if length >= 0 {
                    let start = frame.len();
                    frame.resize(
                        start + usize::try_from(length).map_err(io::Error::other)? + 2,
                        0,
                    );
                    stream.read_exact(&mut frame[start..]).await?;
                }
            }
            b'+' | b'-' | b':' => {}
            other => return Err(io::Error::other(format!("Unexpected RESP2 prefix {other}"))),
        }
    }
    Ok(frame)
}
