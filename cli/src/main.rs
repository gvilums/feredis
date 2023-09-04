use feredis_core::item::RedisItem;
use std::io::Write;
use std::io::Read;

fn main() {
    let n = 1; // Number of threads to spawn
    let mut handles = vec![];

    for _ in 0..n {
        handles.push(std::thread::spawn(|| {
            let mut stream = std::net::TcpStream::connect("localhost:9000").unwrap();
            let mut buf = Vec::new();

            let iters  = 1000;

            let begin = std::time::Instant::now();
            
            let mut discard = Vec::new();
            println!("Sending {} SET commands", iters);
            for i in 0..iters {
                // let cmd = RedisItem::Array(vec![
                //     RedisItem::BulkString("SET".to_string()),
                //     RedisItem::BulkString("foo".to_string()),
                //     RedisItem::BulkString("bar".to_string()),
                // ]);
                let cmd = RedisItem::Array(vec![RedisItem::BulkString("PING".to_string())]);
                cmd.serialize(&mut buf);
                stream.write_all(buf.as_slice()).unwrap();
                let read = stream.read(&mut discard).unwrap();
                println!("read: {}", read);
            }

            let end = std::time::Instant::now();
            let elapsed = end.duration_since(begin);
            let elapsed_ms = elapsed.as_secs() * 1000 + elapsed.subsec_millis() as u64;
            println!("{} ms", elapsed_ms);
            println!("{} ops/s", iters * 1000 / elapsed_ms);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

}
