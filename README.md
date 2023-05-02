# AckUDP

A bit more reliable version of UDP written in Rust.

## How to use?

    use std::{io, thread, time::Duration};
    
    use ack_udp::AckUdp;
    
    fn main() -> io::Result<()> {
	  // Creating sender and receiver sockets
      let mut sender = AckUdp::new("127.0.0.1:9023".parse().unwrap())?;
      let mut receiver = AckUdp::new("127.0.0.1:9024".parse().unwrap())?;
    
      let message = String::from("test").as_bytes().to_vec();
	  
      let status = sender.send(&message, "127.0.0.1:9024".parse().unwrap())?;
      println!("{:?}", status); // Printing the status of the send
    
      thread::sleep(Duration::from_millis(5)); // Simulating the wait time
    
	  // Receiving data
      let (_, datagram) = receiver.recv().unwrap(); 
      println!("{:?}, {:?}", datagram, status);
    
      Ok(())
    }
