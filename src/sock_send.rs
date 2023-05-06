use std::sync::Arc;
use std::{net::SocketAddr, io};
use tokio::net::UdpSocket;
use tokio::task;

pub trait SockSend {
  fn sock_send(&self, buf: Vec<u8>, address: SocketAddr);
}

async fn send(socket: Arc<UdpSocket>, buf: Vec<u8>, address: SocketAddr) -> io::Result<usize> {
  let a = socket.send_to(&buf, address).await?;

  Ok(a)
}

impl SockSend for Arc<UdpSocket> {
  fn sock_send(&self, buf: Vec<u8>, address: SocketAddr) {
    let socket = self.clone();

    task::spawn(send(socket, buf, address));
  }
}