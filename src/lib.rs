use std::{
  sync::{
    Arc, 
    mpsc::{
      self, 
      Sender
    }
  }, 
  net::{
    UdpSocket, 
    SocketAddr
  }, 
  io, 
  collections::{HashMap, VecDeque, HashSet}, 
  thread, time::{Duration}
};

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

pub struct AckUdp {
  pub sock: UdpSocket,

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
  pub fn new(address: SocketAddr) -> io::Result<AckUdp> {
    let sock = UdpSocket::bind(address)?;
    sock.set_nonblocking(true)?;

    let (listener_sender, listener_receiver) = mpsc::channel();
    let (income_checker_sender, income_checker_receiver) = mpsc::channel();
    let (outcome_checker_sender, outcome_checker_receiver) = mpsc::channel();
    let (incoming_queue_sender, incoming_queue_receiver) = mpsc::channel();

    let instance = AckUdp {
      sock: sock.try_clone()?,
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

    let c_sock = sock.try_clone()?;
    let c_ready_to_read_datagrams = instance.ready_to_read_datagrams.clone();
    let c_pending_in_datagrams = instance.pending_in_datagrams.clone();
    let c_pending_out_datagrams = instance.pending_out_datagrams.clone();
    let c_out_datagrams_status_links = instance.out_datagrams_status_links.clone();

    // Check for dropped INcome datagrams
    let c2_pending_in_datagrams = instance.pending_in_datagrams.clone();
    thread::spawn(move || -> io::Result<()> {
      loop {
        if let Ok(()) = income_checker_receiver.try_recv() {
          break;
        }

        let datagrams = c2_pending_in_datagrams.lock().clone();
        for (id, datagram) in datagrams.into_iter() {
          
          let start_time = datagram.last_active.time();
          let end_time = Utc::now().time();
          let diff = end_time - start_time;
          
          // println!("IN total_segments: {}, got: {}, diff: {}", datagram.segments_count, datagram.segments.lock().len(), diff.num_seconds());
          if diff.num_seconds() >= 30 {
            c2_pending_in_datagrams.lock().remove(&id);
          }
        }
        thread::sleep(Duration::from_millis(10));
      }

      Ok(())
    });

    // Check for dropped OUTcome datagrams and resend segments for datagrams that are not already dropped
    let c2_sock = sock.try_clone()?;
    let c2_pending_out_datagrams = instance.pending_out_datagrams.clone();
    let c2_out_datagrams_status_links = instance.out_datagrams_status_links.clone();
    thread::spawn(move || -> io::Result<()> {
      loop {
        if let Ok(()) = outcome_checker_receiver.try_recv() {
          break;
        }

        let datagrams = c2_pending_out_datagrams.lock().clone();
        for (id, datagram) in datagrams.into_iter() {
          let start_time = datagram.last_active.time();
          let end_time = Utc::now().time();
          let diff = end_time - start_time;
          
          // println!("failures: {}, sent: {}, total_segments: {}, acks: {}", datagram.checks_failure_count, datagram.is_fully_sent, datagram.segments_count, datagram.segments_acks.len());
          if diff.num_seconds() >= 5 && datagram.is_fully_sent {
            if datagram.checks_failure_count < 3 {
              let mut cdatagram = datagram.clone();
              let cc2_sock = c2_sock.try_clone()?;
              let cc2_pending_out_datagrams = c2_pending_out_datagrams.clone();

              cdatagram.checks_failure_count += 1;
              cdatagram.is_fully_sent = false;
              cc2_pending_out_datagrams.lock().insert(id, cdatagram.clone());

              thread::spawn(move || {
                let datagram = cc2_pending_out_datagrams.lock().get(&id).unwrap().clone();

                let non_ack_segments = datagram.get_non_ack_segments();
                // println!("resending packets: {}, total: {}", non_ack_segments.len(), datagram.segments_count);
                for packet in non_ack_segments {
                  let packet_bytes: Vec<u8> = packet.into();
                  cc2_sock.sock_send(&packet_bytes, datagram.address);
                  thread::sleep(Duration::from_micros(100));
                }

                let mut c2_datagram = cc2_pending_out_datagrams.lock().get(&id).unwrap().clone();
                c2_datagram.last_active = Utc::now();
                c2_datagram.is_fully_sent = true;
                cc2_pending_out_datagrams.lock().insert(id, c2_datagram);
              });
            }
            else {
              c2_pending_out_datagrams.lock().remove(&id);
              if c2_out_datagrams_status_links.lock().contains_key(&id) {
                c2_out_datagrams_status_links.lock().get(&id).unwrap().lock().0 = AckUdpDatagramOutStatusEnum::Dropped;
              }
            }
          }
        }
        thread::sleep(Duration::from_millis(100));
      }

      Ok(())
    });

    let c_incoming_queue = instance.incoming_queue.clone();
    thread::spawn(move || -> io::Result<()> {
      loop {
        if let Ok(()) = listener_receiver.try_recv() {
          break;
        }

        let mut buf = [0; 412];
        let received = match c_sock.recv_from(&mut buf) {
          Ok(v) => Some(v),
          Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => None,
          Err(e) => panic!("encountered IO error: {e}"),
        };
        
        if received.is_some() {
          c_incoming_queue.lock().push_back((received.unwrap().1, buf.to_vec()));
        }
      }

      Ok(())
    });

    let cc_sock = sock.try_clone()?;
    let cc_ready_to_read_datagrams = c_ready_to_read_datagrams.clone();
    let cc_pending_in_datagrams = c_pending_in_datagrams.clone();
    let cc_pending_out_datagrams = c_pending_out_datagrams.clone();
    let cc_out_datagrams_status_links = c_out_datagrams_status_links.clone();
    let c_incoming_queue = instance.incoming_queue.clone();
    thread::spawn(move || -> io::Result<()> {
      loop {
        if let Ok(()) = incoming_queue_receiver.try_recv() {
          break;
        }

        let v = c_incoming_queue.lock().pop_front();
        if v.is_none() {
          thread::sleep(Duration::from_millis(10));
          continue;
        }
        
        let (src_addr, buf) = v.unwrap();
        
        let packet = AckUdpPacket::from(buf.to_vec());
  
        // Single INcome type Datagram
        if packet.total_segments == 1 && packet.ack == 0 {
          cc_sock.sock_send(&AckUdpPacket::new_ack(packet.datagram_id, vec![0]), src_addr);
          cc_ready_to_read_datagrams.lock().push_back((src_addr, packet.payload));

          continue;
        }
  
        // Splitted INcome type Datagram
        if packet.total_segments > 1 && packet.ack == 0 {
          if cc_pending_in_datagrams.lock().contains_key(&packet.datagram_id) {
            let mut datagram = cc_pending_in_datagrams.lock().get(&packet.datagram_id).unwrap().clone();

            datagram.segments.lock().insert(packet.seg_index, packet.clone());
            datagram.segments_got.push(packet.seg_index);
            datagram.last_active = Utc::now();
  
            let got_segments = datagram.segments.lock().len();
            if packet.total_segments > 100 {
              if got_segments % 100 == 0 || got_segments > (datagram.segments_count - 100).into() {
                let last_packets = {
                  if datagram.segments_got.len() <= 100 {
                    datagram.clone().segments_got
                  }
                  else {
                    datagram.clone().segments_got[datagram.segments_got.len()-100..].to_vec()
                  }
                };
  
                cc_sock.sock_send(&AckUdpPacket::new_ack(packet.datagram_id, last_packets), src_addr);
              }
            }
            else {
              cc_sock.sock_send(&AckUdpPacket::new_ack(packet.datagram_id, vec![packet.seg_index]), src_addr);
            }
  
            if datagram.segments_count as usize == got_segments {
              let payload = datagram.form_payload();
              cc_ready_to_read_datagrams.lock().push_back((src_addr, payload));
              cc_pending_in_datagrams.lock().remove(&packet.datagram_id);
            }
            else {
              cc_pending_in_datagrams.lock().insert(packet.datagram_id, datagram);
            }
          }
          else {
            let segments = Arc::new(Mutex::new(HashMap::new()));
            segments.lock().insert(packet.seg_index, packet.clone());
  
            let segments_got = vec![packet.seg_index];
    
            cc_pending_in_datagrams.lock().insert(packet.datagram_id, AckUdpDatagram { 
              id: packet.datagram_id, 
              address: src_addr,
              segments_count: packet.total_segments, 
              segments, 
              segments_got,
              segments_acks: HashSet::new(), 
              last_active: Utc::now(),
              checks_failure_count: 0,
              is_fully_sent: false
            });
          }
  
          continue;
        }
  
        // Received ACK packet for one of segments
        if packet.ack == 1 {
          if cc_pending_out_datagrams.lock().contains_key(&packet.datagram_id) {
            let mut datagram = cc_pending_out_datagrams.lock().get(&packet.datagram_id).unwrap().to_owned();
  
            let acks = packet.get_acks();
            let is_full_ack = datagram.ack_segment(acks);
            
            if is_full_ack {
              cc_pending_out_datagrams.lock().remove(&packet.datagram_id);
              if cc_out_datagrams_status_links.lock().contains_key(&packet.datagram_id) {
                cc_out_datagrams_status_links.lock().get(&packet.datagram_id).unwrap().lock().0 = AckUdpDatagramOutStatusEnum::Succeeded;
                cc_out_datagrams_status_links.lock().remove(&packet.datagram_id);
              }
            }
            else {
              datagram.last_active = Utc::now();
              cc_pending_out_datagrams.lock().insert(packet.datagram_id, datagram);
            }
          }
  
          continue;
        }

        thread::sleep(Duration::from_millis(1));
      }

      Ok(())
    });

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
        a.ceil() as u16
      };

      let segments = Arc::new(Mutex::new(HashMap::new()));

      for index in 0..segments_count {
        let start = 400 * index as usize;
        let end = {
          let v = start + 400;
          if v > buf.len() {
            buf.len()
          }
          else {
            v
          }
        };
        let payload = &buf[start..end];
        segments.lock().insert(index, AckUdpPacket { 
          datagram_id, 
          seg_index: index, 
          total_segments: segments_count, 
          ack: 0, 
          payload_size: payload.len() as u16, 
          payload: payload.to_vec()
        });
      }


      let datagram = AckUdpDatagram {
        id: datagram_id,
        address,
        segments_count: segments_count,
        segments: segments.clone(),
        segments_got: vec![],
        segments_acks: HashSet::new(),
        last_active: Utc::now(),
        checks_failure_count: 0,
        is_fully_sent: false
      };

      self.pending_out_datagrams.lock().insert(datagram_id, datagram.clone());
      self.out_datagrams_status_links.lock().insert(datagram_id, status.clone());
      
      for (_, packet) in segments.lock().clone().into_iter() {
        let packet_bytes: Vec<u8> = packet.to_owned().into();
        self.sock.sock_send(&packet_bytes, address);
        thread::sleep(Duration::from_micros(100));
      }

      let mut sent_datagram = self.pending_out_datagrams.lock().get(&datagram_id).unwrap().clone();
      sent_datagram.is_fully_sent = true;
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
      segments.lock().insert(1, packet.clone());

      let datagram = AckUdpDatagram {
        id: datagram_id,
        address,
        segments_count: 1,
        segments,
        segments_got: vec![],
        segments_acks: HashSet::new(),
        last_active: Utc::now(),
        checks_failure_count: 0,
        is_fully_sent: true
      };

      self.pending_out_datagrams.lock().insert(datagram_id, datagram);
      self.out_datagrams_status_links.lock().insert(datagram_id, status.clone());
      
      let packet_bytes: Vec<u8> = packet.into();
      self.sock.sock_send(&packet_bytes, address);

      Ok(status.clone())
    }
  }
}