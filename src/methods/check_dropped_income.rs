use std::{io, sync::Arc, collections::HashMap};

use chrono::Utc;
use parking_lot::Mutex;
use tokio::sync::mpsc::Receiver;

use crate::{types::AckUdpDatagram, AckUdp};

impl AckUdp {
  pub async fn check_dropped_income(
    mut income_checker_receiver: Receiver<()>, 
    pending_in_datagrams: Arc<Mutex<HashMap<[u8; 5], AckUdpDatagram>>>
  ) -> io::Result<()> {
    loop {
      if let Ok(()) = income_checker_receiver.try_recv() {
        break;
      }
  
      let datagrams = pending_in_datagrams.lock().clone();
      for (id, datagram) in datagrams.into_iter() {
        
        let start_time = datagram.last_active.time();
        let end_time = Utc::now().time();
        let diff = end_time - start_time;
        
        // println!("IN total_segments: {}, got: {}, diff: {}", datagram.segments_count, datagram.segments.lock().len(), diff.num_seconds());
        if diff.num_seconds() >= 30 {
          pending_in_datagrams.lock().remove(&id);
        }
      }
      tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
  
    Ok(())
  }
}