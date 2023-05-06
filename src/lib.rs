use std::{
  sync::Arc, 
  net::SocketAddr,
  io, 
  collections::{HashMap, VecDeque, HashSet}
};
use tokio::{net::UdpSocket, sync::mpsc::{Sender, self}};


use chrono::Utc;
use parking_lot::Mutex;
use rand::Rng;
use sock_send::SockSend;
use types::{
  AckUdpDatagram, 
  AckUdpDatagramOutStatus, 
  AckUdpDatagramOutStatusEnum, 
  AckUdpPacket
};

mod types;
mod sock_send;
mod methods;

pub struct AckUdp {
  pub sock: Arc<UdpSocket>,

  pub ready_to_read_datagrams: Arc<Mutex<VecDeque<(SocketAddr, Vec<u8>)>>>,
  pub pending_in_datagrams: Arc<Mutex<HashMap<[u8; 5], AckUdpDatagram>>>,
  pub pending_out_datagrams: Arc<Mutex<HashMap<[u8; 5], AckUdpDatagram>>>,

  pub out_datagrams_status_links: Arc<Mutex<HashMap<[u8; 5], Arc<Mutex<AckUdpDatagramOutStatus>>>>>,

  pub kill_listener_channel_sender: Sender<()>,
  pub kill_income_checker_channel_sender: Sender<()>,
  pub kill_outcome_checker_listener_channel_sender: Sender<()>,
  pub kill_incoming_queue_channel_sender: Sender<()>,

  pub incoming_queue: Arc<Mutex<VecDeque<(SocketAddr, Vec<u8>)>>>
}

impl AckUdp {
  pub async fn new(address: SocketAddr) -> io::Result<AckUdp> {
    let sock = Arc::new(UdpSocket::bind(address).await?);

    let (listener_sender, listener_receiver) = mpsc::channel(1);
    let (income_checker_sender, income_checker_receiver) = mpsc::channel(1);
    let (outcome_checker_sender, outcome_checker_receiver) = mpsc::channel(1);
    let (incoming_queue_sender, incoming_queue_receiver) = mpsc::channel(1);

    let instance = AckUdp {
      sock: sock.clone(),
      ready_to_read_datagrams: Arc::new(Mutex::new(VecDeque::new())),
      pending_in_datagrams: Arc::new(Mutex::new(HashMap::new())),
      pending_out_datagrams: Arc::new(Mutex::new(HashMap::new())),
      out_datagrams_status_links: Arc::new(Mutex::new(HashMap::new())),
      kill_listener_channel_sender: listener_sender,
      kill_income_checker_channel_sender: income_checker_sender,
      kill_outcome_checker_listener_channel_sender: outcome_checker_sender,
      kill_incoming_queue_channel_sender: incoming_queue_sender,
      incoming_queue: Arc::new(Mutex::new(VecDeque::new()))
    };

    // Check for dropped INcome datagrams
    tokio::spawn(AckUdp::check_dropped_income(
      income_checker_receiver, 
      instance.pending_in_datagrams.clone())
    );

    // Check for dropped OUTcome datagrams and resend segments for datagrams that are not already dropped
    tokio::spawn(AckUdp::check_dropped_outcome(
      outcome_checker_receiver, 
      instance.pending_out_datagrams.clone(), 
      instance.sock.clone(), 
      instance.out_datagrams_status_links.clone()
    ));

    // Listen for incoming packets on socket
    tokio::spawn(AckUdp::listen_packets(listener_receiver, instance.sock.clone(), instance.incoming_queue.clone()));

    // Proccess packet 
    tokio::spawn(AckUdp::process_packets(
      incoming_queue_receiver, 
      instance.incoming_queue.clone(), 
      instance.sock.clone(), 
      instance.ready_to_read_datagrams.clone(), 
      instance.pending_in_datagrams.clone(), 
      instance.pending_out_datagrams.clone(), 
      instance.out_datagrams_status_links.clone()
    ));

    Ok(instance)
  }

  pub fn recv(&mut self) -> Option<(SocketAddr, Vec<u8>)> {
    self.ready_to_read_datagrams.lock().pop_front()
  }

  pub fn send(&mut self, buf: &[u8], address: SocketAddr ) -> io::Result<Arc<Mutex<AckUdpDatagramOutStatus>>> {
    let datagram_id = rand::thread_rng().gen::<[u8; 5]>();
    let status = Arc::new(Mutex::new(AckUdpDatagramOutStatus(AckUdpDatagramOutStatusEnum::Pending)));
    if buf.len() > 400 {
      let segments_count = {
        let a = buf.len() as f64 / 400 as f64;
        a.ceil() as u32
      };

      let segments = Arc::new(Mutex::new(HashMap::new()));

      let datagram = AckUdpDatagram {
        id: datagram_id,
        address,
        segments_count: segments_count,
        segments: segments.clone(),
        segments_got: Arc::new(Mutex::new(vec![])),
        segments_acks: Arc::new(Mutex::new(HashSet::new())),
        last_active: Utc::now(),
        checks_failure_count: 0
      };

      self.pending_out_datagrams.lock().insert(datagram_id, datagram.clone());
      self.out_datagrams_status_links.lock().insert(datagram_id, status.clone());

      let buffer_length = buf.len();
      for index in 0..segments_count {
        let start = 400 * index as usize;
        let end = {
          let v = start + 400;
          if v > buffer_length {
            buffer_length
          }
          else {
            v
          }
        };
        let payload = &buf[start..end];
        let packet = AckUdpPacket { 
          datagram_id, 
          seg_index: index, 
          total_segments: segments_count, 
          ack: 0, 
          payload_size: payload.len() as u16, 
          payload: payload.to_vec()
        };
        segments.lock().insert(index, packet.clone());
        
        let packet_bytes: Vec<u8> = packet.into();
        self.sock.sock_send(packet_bytes, address);
      }

      let mut sent_datagram = self.pending_out_datagrams.lock().get(&datagram_id).unwrap().clone();
      sent_datagram.last_active = Utc::now();
      self.pending_out_datagrams.lock().insert(datagram_id, sent_datagram);

      return Ok(status.clone());
    }
    else {
      let packet = AckUdpPacket { 
        datagram_id,
        seg_index: 0,
        total_segments: 1,
        ack: 0,
        payload_size: buf.len() as u16, 
        payload: buf.to_vec()
      };

      let segments= Arc::new(Mutex::new(HashMap::new()));
      segments.lock().insert(0, packet.clone());

      let datagram = AckUdpDatagram {
        id: datagram_id,
        address,
        segments_count: 1,
        segments,
        segments_got: Arc::new(Mutex::new(vec![])),
        segments_acks: Arc::new(Mutex::new(HashSet::new())),
        last_active: Utc::now(),
        checks_failure_count: 0
      };

      self.pending_out_datagrams.lock().insert(datagram_id, datagram);
      self.out_datagrams_status_links.lock().insert(datagram_id, status.clone());
      
      let packet_bytes: Vec<u8> = packet.into();
      self.sock.sock_send(packet_bytes, address);

      Ok(status.clone())
    }
  }
}