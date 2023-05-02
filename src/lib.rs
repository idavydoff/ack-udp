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
  collections::{HashMap, VecDeque}, 
  thread, time::Duration
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
}

impl AckUdp {
  pub fn new(address: SocketAddr) -> io::Result<AckUdp> {
    let sock = UdpSocket::bind(address)?;
    sock.set_nonblocking(true)?;

    let (listener_sender, listener_receiver) = mpsc::channel();
    let (income_checker_sender, income_checker_receiver) = mpsc::channel();
    let (outcome_checker_sender, outcome_checker_receiver) = mpsc::channel();

    let instance = AckUdp {
      sock: sock.try_clone()?,
      ready_to_read_datagrams: Arc::new(Mutex::new(VecDeque::new())),
      pending_in_datagrams: Arc::new(Mutex::new(HashMap::new())),
      pending_out_datagrams: Arc::new(Mutex::new(HashMap::new())),
      out_datagrams_status_links: Arc::new(Mutex::new(HashMap::new())),
      kill_listener_channel_sender: listener_sender,
      kill_income_checker_channel_sender: income_checker_sender,
      kill_outcome_checker_listener_channel_sender: outcome_checker_sender,
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

        for (id, datagram) in c2_pending_in_datagrams.lock().clone().into_iter() {
          let start_time = datagram.last_active.time();
          let end_time = Utc::now().time();
          let diff = end_time - start_time;
  
          if diff.num_seconds() >= 5 {
            c2_pending_in_datagrams.lock().remove(&id);
          }
        }
        thread::sleep(Duration::from_millis(100));
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

        for (id, datagram) in c2_pending_out_datagrams.lock().clone().into_iter() {
          let start_time = datagram.last_active.time();
          let end_time = Utc::now().time();
          let diff = end_time - start_time;
          
          if diff.num_seconds() >= 5 {
            if datagram.checks_failure_count < 3 {
              let cdatagram = datagram.clone();
              let cc2_sock = c2_sock.try_clone()?;
              let cc2_pending_out_datagrams = c2_pending_out_datagrams.clone();
              thread::spawn(move || {
                let non_ack_segments = cdatagram.get_non_ack_segments();
                for packet in non_ack_segments {
                  let packet_bytes: Vec<u8> = packet.into();
                  cc2_sock.sock_send(&packet_bytes, cdatagram.address);
                  thread::sleep(Duration::from_millis(1));
                } 
                let mut c_datagram = cdatagram.clone();
                c_datagram.checks_failure_count += 1;
                cc2_pending_out_datagrams.lock().insert(id, c_datagram);
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

        if received.is_none() {
          continue;
        }

        let (_, src_addr) = received.unwrap();

        let packet = AckUdpPacket::from(buf.to_vec());

        // Single INcome type Datagram
        if packet.total_segments == 1 && packet.ack == 0 {
          c_ready_to_read_datagrams.lock().push_back((src_addr, packet.payload));
          c_sock.sock_send(&AckUdpPacket::new_ack(packet.datagram_id, 0), src_addr);

          continue;
        }

        // Splitted INcome type Datagram
        if packet.total_segments > 1 && packet.ack == 0 {
          if c_pending_in_datagrams.lock().contains_key(&packet.datagram_id) {
            let mut datagram = c_pending_in_datagrams.lock().get(&packet.datagram_id).unwrap().clone();
            datagram.segments.insert(packet.seg_index, packet.clone());
            datagram.last_active = Utc::now();

            if datagram.segments_count as usize == datagram.segments.len() {
              let payload = datagram.form_payload();
              c_ready_to_read_datagrams.lock().push_back((src_addr, payload));
              c_pending_in_datagrams.lock().remove(&packet.datagram_id);
            }
            else {
              c_pending_in_datagrams.lock().insert(packet.datagram_id, datagram);
            }
          }
          else {
            let mut segments = HashMap::new();
            segments.insert(packet.seg_index, packet.clone());
     
            c_pending_in_datagrams.lock().insert(packet.datagram_id, AckUdpDatagram { 
              id: packet.datagram_id, 
              address: src_addr,
              segments_count: packet.total_segments, 
              segments, 
              segments_acks: HashMap::new(), 
              last_active: Utc::now(),
              checks_failure_count: 0
            });
          }

          c_sock.sock_send(&AckUdpPacket::new_ack(packet.datagram_id, packet.seg_index), src_addr);
          continue;
        }

        // Received ACK packet for one of segments
        if packet.ack == 1 && c_pending_out_datagrams.lock().contains_key(&packet.datagram_id) {
          let mut datagram = c_pending_out_datagrams.lock().get(&packet.datagram_id).unwrap().to_owned();
          let is_full_ack = datagram.ack_segment(packet.seg_index);
          
          if is_full_ack {
            c_pending_out_datagrams.lock().remove(&packet.datagram_id);
            if c_out_datagrams_status_links.lock().contains_key(&packet.datagram_id) {
              c_out_datagrams_status_links.lock().get(&packet.datagram_id).unwrap().lock().0 = AckUdpDatagramOutStatusEnum::Succeeded;
              c_out_datagrams_status_links.lock().remove(&packet.datagram_id);
            }
          }
          else {
            datagram.last_active = Utc::now();
            c_pending_out_datagrams.lock().insert(packet.datagram_id, datagram);
          }

          continue;
        }

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

      let mut segments: HashMap<u16, AckUdpPacket> = HashMap::new();

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
        segments.insert(index, AckUdpPacket { 
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
        segments_acks: HashMap::new(),
        last_active: Utc::now(),
        checks_failure_count: 0
      };

      self.pending_out_datagrams.lock().insert(datagram_id, datagram);
      self.out_datagrams_status_links.lock().insert(datagram_id, status.clone());

      for (_, packet) in segments {
        let packet_bytes: Vec<u8> = packet.into();
        self.sock.sock_send(&packet_bytes, address);
        thread::sleep(Duration::from_millis(1));
      }

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

      let mut segments: HashMap<u16, AckUdpPacket> = HashMap::new();
      segments.insert(1, packet.clone());

      let datagram = AckUdpDatagram {
        id: datagram_id,
        address,
        segments_count: 1,
        segments,
        segments_acks: HashMap::new(),
        last_active: Utc::now(),
        checks_failure_count: 0
      };

      self.pending_out_datagrams.lock().insert(datagram_id, datagram);
      self.out_datagrams_status_links.lock().insert(datagram_id, status.clone());
      
      let packet_bytes: Vec<u8> = packet.into();
      self.sock.sock_send(&packet_bytes, address);


      Ok(status.clone())
    }
  }
}