// AckUdp Packet Header
// 5 bytes datagram id   2 bytes segment index   2 bytes total segments number   1 byte ACK   2 bytes payload size
// -------------------___---------------------___-----------------------------___----------___--------------------

use std::{collections::{HashMap, HashSet}, io::Cursor, net::SocketAddr};
use chrono::prelude::*;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;

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
  pub segments_count: u16,
  pub segments: HashMap<u16, AckUdpPacket>,
  
  pub segments_got: Vec<u16>,  // Only for INcome datagrams

  pub segments_acks: HashSet<u16>, // Only for OUTcome datagrams
  pub checks_failure_count: u8, // Only for OUTcome datagrams
  pub is_fully_sent: bool, //Only for OUTcome datagrams

  pub last_active: DateTime<Utc>,
}

impl AckUdpDatagram {
  pub fn ack_segment(&mut self, ids: Vec<u16>) -> bool {
    for id in ids {
      self.segments_acks.insert(id);
    }

    self.segments_acks.len() == self.segments_count as usize
  }

  pub fn form_payload(&mut self) -> Vec<u8> {
    let mut res = Vec::new();
    for b in self.segments.keys().sorted() {
      res.extend_from_slice(&self.segments.get(b).unwrap().payload);
    }

    res
  }

  pub fn get_non_ack_segments(&self) -> Vec<AckUdpPacket> {
    let mut res = vec![];

    for (id, packet) in &self.segments {
      if !self.segments_acks.contains(id) {
        res.push(packet.to_owned());
      }
    }

    res
  }
}

#[derive(Debug, Clone)]
pub struct AckUdpPacket {
  pub datagram_id: [u8; 5],
  pub seg_index: u16,
  pub total_segments: u16,
  pub ack: u8,
  pub payload_size: u16,
  pub payload: Vec<u8>,
}

impl AckUdpPacket {
  pub fn new_ack(id: [u8; 5], segs: Vec<u16>) -> Vec<u8> {
    let mut payload = vec![];
  
    for seg in segs {
      let mut wtr = vec![];
      wtr.write_u16::<BigEndian>(seg).unwrap();
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

  pub fn get_acks(&self) -> Vec<u16> {
    let mut res = vec![];
    let tmp: Vec<&[u8]> = self.payload.chunks(2).collect();

    for seg in tmp {
      let rdr = Cursor::new(seg).read_u16::<BigEndian>().unwrap();
      res.push(rdr);
    }

    res
  }
}

impl From<Vec<u8>> for AckUdpPacket {
  fn from(raw_packet: Vec<u8>) -> Self {
    let payload_size: Vec<u8> = raw_packet[10..12].try_into().unwrap();
    let rdr_payload_size = Cursor::new(payload_size).read_u16::<BigEndian>().unwrap();

    let total_segments: Vec<u8> = raw_packet[7..9].try_into().unwrap();
    let rdr_total_segments = Cursor::new(total_segments).read_u16::<BigEndian>().unwrap();

    let seg_index: Vec<u8> = raw_packet[5..7].try_into().unwrap();
    let rdr_seg_index = Cursor::new(seg_index).read_u16::<BigEndian>().unwrap();

    AckUdpPacket { 
      datagram_id: raw_packet[..5].try_into().unwrap(), 
      seg_index: rdr_seg_index, 
      ack: raw_packet[9],
      payload_size: rdr_payload_size, 
      total_segments: rdr_total_segments,
      payload: raw_packet[12..(rdr_payload_size + 12) as usize].try_into().unwrap(),
    }
  }
}

impl Into<Vec<u8>> for AckUdpPacket {
  fn into(self) -> Vec<u8> {
    let mut result: Vec<u8> = vec![];
    result.extend_from_slice(&self.datagram_id);
    
    let mut wtr_seg_index = vec![];
    wtr_seg_index.write_u16::<BigEndian>(self.seg_index).unwrap();
    result.extend_from_slice(&wtr_seg_index);

    let mut wtr_total_segments = vec![];
    wtr_total_segments.write_u16::<BigEndian>(self.total_segments).unwrap();
    result.extend_from_slice(&wtr_total_segments);

    result.push(self.ack);

    let mut wtr_payload_size = vec![];
    wtr_payload_size.write_u16::<BigEndian>(self.payload_size).unwrap();
    result.extend_from_slice(&wtr_payload_size);

    result.extend_from_slice(&self.payload);

    result
  }
}
