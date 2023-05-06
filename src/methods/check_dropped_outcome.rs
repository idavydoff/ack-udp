use std::{sync::Arc, collections::HashMap};

use chrono::Utc;
use parking_lot::Mutex;
use tokio::{net::UdpSocket, sync::mpsc::Receiver};

use crate::{types::{AckUdpDatagram, AckUdpDatagramOutStatusEnum, AckUdpDatagramOutStatus}, sock_send::SockSend, AckUdp};

impl AckUdp {
  pub async fn check_dropped_outcome(
    mut outcome_checker_receiver: Receiver<()>, 
    pending_out_datagrams: Arc<Mutex<HashMap<[u8; 5], AckUdpDatagram>>>,
    socket: Arc<UdpSocket>,
    out_datagrams_status_links: Arc<Mutex<HashMap<[u8; 5], Arc<Mutex<AckUdpDatagramOutStatus>>>>>
  ) {
    loop {
      if let Ok(()) = outcome_checker_receiver.try_recv() {
        break;
      }
      
      let datagrams = pending_out_datagrams.lock().clone();
      for (id, datagram) in datagrams.into_iter() {
        let start_time = datagram.last_active.time();
        let end_time = Utc::now().time();
        let diff = end_time - start_time;
        
        println!(
          "failures: {}, total_segments: {}, acks: {}, diff: {}", 
          datagram.checks_failure_count, datagram.segments_count, datagram.segments_acks.lock().len(), diff.num_milliseconds());
        if diff.num_milliseconds() >= 500 {
          if datagram.checks_failure_count < 200 {
            let mut cdatagram = datagram.clone();
            let cc2_pending_out_datagrams = pending_out_datagrams.clone();
  
            cdatagram.checks_failure_count += 1;
            cc2_pending_out_datagrams.lock().insert(id, cdatagram.clone());
  
            tokio::spawn(resend_packets(pending_out_datagrams.clone(), socket.clone(), id));
          }
          else {
            pending_out_datagrams.lock().remove(&id);
            if out_datagrams_status_links.lock().contains_key(&id) {
              out_datagrams_status_links.lock().get(&id).unwrap().lock().0 = AckUdpDatagramOutStatusEnum::Dropped;
            }
          }
        }
      }
      tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
  }
}

async fn resend_packets(
  pending_out_datagrams: Arc<Mutex<HashMap<[u8; 5], AckUdpDatagram>>>,
  socket: Arc<UdpSocket>,
  id: [u8; 5]
) {
  let datagram = match pending_out_datagrams.lock().get(&id) {
    Some(v) => v.clone(),
    None => return
  };
  let non_ack_segments = datagram.get_non_ack_segments();
  println!("resending packets: {}, total: {}", non_ack_segments.len(), datagram.segments_count);
  
  for packet in non_ack_segments {
    let packet_bytes: Vec<u8> = packet.into();
    socket.sock_send(packet_bytes, datagram.address);
  }

  let mut datagram = match pending_out_datagrams.lock().get(&id) {
    Some(v) => v.clone(),
    None => return
  };
  
  datagram.last_active = Utc::now();
  pending_out_datagrams.lock().insert(id, datagram);
}