use std::{sync::Arc, collections::{VecDeque, HashSet, HashMap}, net::SocketAddr};

use chrono::Utc;
use parking_lot::Mutex;
use tokio::{sync::mpsc::Receiver, net::UdpSocket};

use crate::{AckUdp, types::{AckUdpPacket, AckUdpDatagram, AckUdpDatagramOutStatusEnum, AckUdpDatagramOutStatus}, sock_send::SockSend};

impl AckUdp {
  pub async fn process_packets(
    mut incoming_queue_receiver: Receiver<()>, 
    incoming_queue: Arc<Mutex<VecDeque<(SocketAddr, Vec<u8>)>>>,
    socket: Arc<UdpSocket>,
    ready_to_read_datagrams: Arc<Mutex<VecDeque<(SocketAddr, Vec<u8>)>>>,
    pending_in_datagrams: Arc<Mutex<HashMap<[u8; 5], AckUdpDatagram>>>,
    pending_out_datagrams: Arc<Mutex<HashMap<[u8; 5], AckUdpDatagram>>>,
    out_datagrams_status_links: Arc<Mutex<HashMap<[u8; 5], Arc<Mutex<AckUdpDatagramOutStatus>>>>>,
  ) {
    loop {
      if let Ok(()) = incoming_queue_receiver.try_recv() {
        break;
      }
  
      let v = incoming_queue.lock().pop_front();
      if v.is_none() {
        continue;
      }
      
      let (src_addr, buf) = v.unwrap();
      
      let packet = AckUdpPacket::from(buf.to_vec());
  
      // Single INcome type Datagram
      if packet.total_segments == 1 && packet.ack == 0 {
        socket.sock_send(AckUdpPacket::new_ack(packet.datagram_id, vec![0]), src_addr);
        ready_to_read_datagrams.lock().push_back((src_addr, packet.payload));
  
        continue;
      }
  
      // Splitted INcome type Datagram
      if packet.total_segments > 1 && packet.ack == 0 {
        if pending_in_datagrams.lock().contains_key(&packet.datagram_id) {
          let mut datagram = pending_in_datagrams.lock().get(&packet.datagram_id).unwrap().clone();
  
          datagram.segments.lock().insert(packet.seg_index, packet.clone());
          datagram.segments_got.lock().push(packet.seg_index);
          datagram.last_active = Utc::now();
  
          let got_segments = datagram.segments.lock().len();
          if packet.total_segments > 100 {
            if got_segments % 100 == 0 || got_segments >= (datagram.segments_count - 10) as usize {
              let last_packets = {
                if datagram.segments_got.lock().len() <= 100 {
                  datagram.clone().segments_got.lock().clone()
                }
                else {
                  let start = datagram.segments_got.lock().len()-100;
                  datagram.segments_got.lock()[start..].to_vec()
                }
              };
              socket.sock_send(AckUdpPacket::new_ack(packet.datagram_id, last_packets), src_addr);
            }
          }
          else {
            socket.sock_send(AckUdpPacket::new_ack(packet.datagram_id, vec![packet.seg_index]), src_addr);
          }
  
          if datagram.segments_count as usize == got_segments {
            let payload = datagram.form_payload();
            ready_to_read_datagrams.lock().push_back((src_addr, payload));
            pending_in_datagrams.lock().remove(&packet.datagram_id);
            socket.sock_send(AckUdpPacket::new_ack(packet.datagram_id, vec![packet.total_segments - 1]), src_addr);
          }
          else {
            pending_in_datagrams.lock().insert(packet.datagram_id, datagram);
          }
        }
        else {
          let segments = Arc::new(Mutex::new(HashMap::new()));
          segments.lock().insert(packet.seg_index, packet.clone());
  
          let segments_got = Arc::new(Mutex::new(vec![packet.seg_index]));
  
          pending_in_datagrams.lock().insert(packet.datagram_id, AckUdpDatagram { 
            id: packet.datagram_id, 
            address: src_addr,
            segments_count: packet.total_segments, 
            segments, 
            segments_got,
            segments_acks: Arc::new(Mutex::new(HashSet::new())), 
            last_active: Utc::now(),
            checks_failure_count: 0
          });

          socket.sock_send(AckUdpPacket::new_ack(packet.datagram_id, vec![packet.seg_index]), src_addr);
        }
  
        continue;
      }
  
      // Received ACK packet for one of segments
      if packet.ack == 1 {
        if pending_out_datagrams.lock().contains_key(&packet.datagram_id) {
          let mut datagram = pending_out_datagrams.lock().get(&packet.datagram_id).unwrap().to_owned();
  
          let acks = packet.get_acks();
          let is_full_ack = datagram.ack_segment(acks);

          if is_full_ack {
            pending_out_datagrams.lock().remove(&packet.datagram_id);
            if out_datagrams_status_links.lock().contains_key(&packet.datagram_id) {
              out_datagrams_status_links.lock().get(&packet.datagram_id).unwrap().lock().0 = AckUdpDatagramOutStatusEnum::Succeeded;
              out_datagrams_status_links.lock().remove(&packet.datagram_id);
            }
          }
          else {
            datagram.checks_failure_count = 0;
            datagram.last_active = Utc::now();
            pending_out_datagrams.lock().insert(packet.datagram_id, datagram);
          }
        }
  
        continue;
      }
    }
  }
}