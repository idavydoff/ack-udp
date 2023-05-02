use std::{net::{SocketAddr, UdpSocket}, io, thread, time::Duration};

pub trait SockSend {
  fn sock_send(&self, buf: &[u8], address: SocketAddr) -> Option<usize>;
}

impl SockSend for UdpSocket {
  fn sock_send(&self, buf: &[u8], address: SocketAddr) -> Option<usize> {
    loop {
      let res: Option<usize> = match self.send_to(buf, address) {
        Ok(v) => Some(v),
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
          thread::sleep(Duration::from_millis(10));
          continue;
        },
        Err(_) => None,
      };

      return res
    }
  }
}