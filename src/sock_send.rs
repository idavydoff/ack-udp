use std::{net::{SocketAddr, UdpSocket}, io, thread, time::Duration};

pub trait SockSend {
  fn sock_send(&self, buf: &[u8], address: SocketAddr);
}

impl SockSend for UdpSocket {
  fn sock_send(&self, buf: &[u8], address: SocketAddr) {
    let socket = self.try_clone().unwrap();
    let buf2 = buf.to_vec();

    thread::spawn(move || {
      loop {
        let res: Option<usize> = match socket.send_to(&buf2, address) {
          Ok(v) => Some(v),
          Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
            // thread::sleep(Duration::from_millis(1));
            continue;
          },
          Err(_) => None,
        };
  
        return res
      }
    });
  }
}