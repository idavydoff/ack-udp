use std::{sync::Arc, io, collections::VecDeque, net::SocketAddr};

use parking_lot::Mutex;
use tokio::{net::UdpSocket, sync::mpsc::Receiver};

use crate::AckUdp;

impl AckUdp {
  pub async fn listen_packets(
    mut listener_receiver: Receiver<()>, 
    socket: Arc<UdpSocket>, 
    incoming_queue: Arc<Mutex<VecDeque<(SocketAddr, Vec<u8>)>>>
  ) {
    loop {
      if let Ok(()) = listener_receiver.try_recv() {
        break;
      }
  
      let mut buf = [0; 416];
      let received = match socket.recv_from(&mut buf).await {
        Ok(v) => Some(v),
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => None,
        Err(e) => panic!("encountered IO error: {e}"),
      };
      
      if received.is_some() {
        incoming_queue.lock().push_back((received.unwrap().1, buf.to_vec()));
      }
    }
  }
}