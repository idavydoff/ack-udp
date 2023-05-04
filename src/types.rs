// AckUdp Packet Header
// 5 bytes datagram id   4 bytes segment index   4 bytes total segments number   1 byte ACK   2 bytes payload size
// -------------------___---------------------___-----------------------------___----------___--------------------

use std::{collections::{HashMap, HashSet}, io::Cursor, net::SocketAddr, sync::Arc};
use chrono::prelude::*;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use parking_lot::Mutex;

#[derive(Debug)]
pub enum AckUdpDatagramOutStatusEnum {
  Pending,
  Dropped,
  Succeeded
}

#[derive(Debug)]
pub struct AckUdpDatagramOutStatus(pub AckUdpDatagramOutStatusEnum);

#[derive(Debug, Clone)]
pub struct AckUdpDatagram {
  pub id: [u8; 5],
  pub address: SocketAddr,
  pub segments_count: u32,
  pub segments: Arc<Mutex<HashMap<u32, AckUdpPacket>>>,
  
  pub segments_got:  Arc<Mutex<Vec<u32>>>,  // Only for INcome datagrams

  pub segments_acks:  Arc<Mutex<HashSet<u32>>>, // Only for OUTcome datagrams
  pub checks_failure_count: u16, // Only for OUTcome datagrams

  pub last_active: DateTime<Utc>,
}

impl AckUdpDatagram {
  pub fn ack_segment(&mut self, ids: Vec<u32>) -> bool {
    for id in ids {
      self.segments_acks.lock().insert(id);
    }

    self.segments_acks.lock().len() == self.segments_count as usize
  }

  pub fn form_payload(&mut self) -> Vec<u8> {
    let mut res = Vec::new();
    for b in 0..self.segments_count {
      res.extend_from_slice(&self.segments.lock().get(&b).unwrap().payload);
    }

    res
  }

  pub fn get_non_ack_segments(&self) -> Vec<AckUdpPacket> {
    let mut res = vec![];

    let length = self.segments.lock().len();
    for id in 0..length {
      if !self.segments_acks.lock().contains(&(id as u32)) {
        res.push(self.segments.lock().get(&(id as u32)).unwrap().to_owned());
      }
    }

    res
  }
}

#[derive(Debug, Clone)]
pub struct AckUdpPacket {
  pub datagram_id: [u8; 5],
  pub seg_index: u32,
  pub total_segments: u32,
  pub ack: u8,
  pub payload_size: u16,
  pub payload: Vec<u8>,
}

impl AckUdpPacket {
  pub fn new_ack(id: [u8; 5], segs: Vec<u32>) -> Vec<u8> {
    let mut payload = vec![];
  
    for seg in segs {
      let mut wtr = vec![];
      wtr.write_u32::<BigEndian>(seg).unwrap();
      payload.extend_from_slice(&wtr);
    }

    let packet = AckUdpPacket { 
      datagram_id: id,
      seg_index: 0,
      total_segments: 1,
      ack: 1,
      payload_size: payload.len() as u16, 
      payload
    };

    let bytes: Vec<u8> = packet.into();

    bytes
  }

  pub fn get_acks(&self) -> Vec<u32> {
    let mut res = vec![];
    let tmp: Vec<&[u8]> = self.payload.chunks(4).collect();

    for seg in tmp {
      let rdr = Cursor::new(seg).read_u32::<BigEndian>().unwrap();
      res.push(rdr);
    }

    res
  }
}

impl From<Vec<u8>> for AckUdpPacket {
  fn from(raw_packet: Vec<u8>) -> Self {
    let payload_size: Vec<u8> = raw_packet[14..16].try_into().unwrap();
    let rdr_payload_size = Cursor::new(payload_size).read_u16::<BigEndian>().unwrap();

    let total_segments: Vec<u8> = raw_packet[9..13].try_into().unwrap();
    let rdr_total_segments = Cursor::new(total_segments).read_u32::<BigEndian>().unwrap();

    let seg_index: Vec<u8> = raw_packet[5..9].try_into().unwrap();
    let rdr_seg_index = Cursor::new(seg_index).read_u32::<BigEndian>().unwrap();

    AckUdpPacket { 
      datagram_id: raw_packet[..5].try_into().unwrap(), 
      seg_index: rdr_seg_index, 
      ack: raw_packet[13],
      payload_size: rdr_payload_size, 
      total_segments: rdr_total_segments,
      payload: raw_packet[16..(rdr_payload_size + 16) as usize].try_into().unwrap(),
    }
  }
}

impl Into<Vec<u8>> for AckUdpPacket {
  fn into(self) -> Vec<u8> {
    let mut result: Vec<u8> = vec![];
    result.extend_from_slice(&self.datagram_id);
    
    let mut wtr_seg_index = vec![];
    wtr_seg_index.write_u32::<BigEndian>(self.seg_index).unwrap();
    result.extend_from_slice(&wtr_seg_index);

    let mut wtr_total_segments = vec![];
    wtr_total_segments.write_u32::<BigEndian>(self.total_segments).unwrap();
    result.extend_from_slice(&wtr_total_segments);

    result.push(self.ack);

    let mut wtr_payload_size = vec![];
    wtr_payload_size.write_u16::<BigEndian>(self.payload_size).unwrap();
    result.extend_from_slice(&wtr_payload_size);

    result.extend_from_slice(&self.payload);

    result
  }
}
