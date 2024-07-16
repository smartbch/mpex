use std::sync::Arc;

use crate::def::BIG_BUF_SIZE;

pub type BigBuf = [u8; BIG_BUF_SIZE];

pub fn new_big_buf_boxed() -> Box<[u8; BIG_BUF_SIZE]> {
    Box::new([0u8; BIG_BUF_SIZE])
}
pub fn new_big_buf_arc() -> Arc<[u8; BIG_BUF_SIZE]> {
    Arc::new([0u8; BIG_BUF_SIZE])
}

pub mod hasher {
    use sha2::{Digest, Sha256};
    pub fn hash<T: AsRef<[u8]>>(a: T) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(a);
        hasher.finalize().into()
    }

    pub fn hash1<T: AsRef<[u8]>>(level: u8, a: T) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update([level]);
        hasher.update(a);
        hasher.finalize().into()
    }

    pub fn hash2<T: AsRef<[u8]>>(children_level: u8, a: T, b: T) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update([children_level]);
        hasher.update(a);
        hasher.update(b);
        hasher.finalize().into()
    }

    pub fn hash2x<T: AsRef<[u8]>>(children_level: u8, a: T, b: T, exchange_ab: bool) -> [u8; 32] {
        if exchange_ab {
            hash2(children_level, b, a)
        } else {
            hash2(children_level, a, b)
        }
    }

    pub fn node_hash_inplace<T: AsRef<[u8]>>(
        children_level: u8,
        target: &mut [u8],
        src_a: T,
        src_b: T,
    ) {
        let mut hasher = Sha256::new();
        hasher.update([children_level]);
        hasher.update(src_a);
        hasher.update(src_b);
        target.copy_from_slice(&hasher.finalize());
    }
}

pub mod codec {
    use byteorder::{ByteOrder, LittleEndian};

    pub fn decode_le_i64(v: &Vec<u8>) -> i64 {
        LittleEndian::read_i64(&v[0..8])
    }
    pub fn decode_le_u64(v: &Vec<u8>) -> u64 {
        LittleEndian::read_u64(&v[0..8])
    }
    pub fn encode_le_u64(n: u64) -> Vec<u8> {
        n.to_le_bytes().to_vec()
    }
    pub fn encode_le_i64(n: i64) -> Vec<u8> {
        n.to_le_bytes().to_vec()
    }
}

pub mod ringchannel {
    use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

    pub struct Producer<T: Clone> {
        fwd_sender: SyncSender<T>,
        bck_receiver: Receiver<T>,
    }

    pub struct Consumer<T: Clone> {
        bck_sender: SyncSender<T>,
        fwd_receiver: Receiver<T>,
    }

    pub fn new<T: Clone>(size: usize, t: &T) -> (Producer<T>, Consumer<T>) {
        let (fwd_sender, fwd_receiver) = sync_channel(size);
        let (bck_sender, bck_receiver) = sync_channel(size);
        for _ in 0..size {
            bck_sender.send(t.clone()).unwrap();
        }
        let prod = Producer {
            fwd_sender,
            bck_receiver,
        };
        let cons = Consumer {
            bck_sender,
            fwd_receiver,
        };
        (prod, cons)
    }

    impl<T: Clone> Producer<T> {
        pub fn produce(&mut self, t: T) {
            self.fwd_sender.send(t).unwrap();
        }
        pub fn receive_returned(&mut self) -> T {
            self.bck_receiver.recv().unwrap()
        }
    }

    impl<T: Clone> Consumer<T> {
        pub fn consume(&mut self) -> T {
            self.fwd_receiver.recv().unwrap()
        }
        pub fn send_returned(&mut self, t: T) {
            self.bck_sender.send(t).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::codec::*;
    use super::hasher::*;

    #[test]
    fn test_hash2() {
        assert_eq!(
            hex::encode(hash2(8, "hello", "world")),
            "8e6fc50a3f98a3c314021b89688ca83a9b5697ca956e211198625fc460ddf1e9"
        );

        assert_eq!(
            hex::encode(hash2x(8, "world", "hello", true)),
            "8e6fc50a3f98a3c314021b89688ca83a9b5697ca956e211198625fc460ddf1e9"
        );
    }

    #[test]
    fn test_node_hash_inplace() {
        let mut target: [u8; 32] = [0; 32];
        node_hash_inplace(8, &mut target, "hello", "world");
        assert_eq!(
            hex::encode(&target),
            "8e6fc50a3f98a3c314021b89688ca83a9b5697ca956e211198625fc460ddf1e9"
        );
    }

    #[test]
    fn test_encode_decode_n64() {
        let v = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        assert_eq!(decode_le_i64(&v), -8613303245920329199);
        assert_eq!(decode_le_u64(&v), 0x8877665544332211);
        assert_eq!(encode_le_i64(-8613303245920329199), v);
        assert_eq!(encode_le_u64(0x8877665544332211), v);
    }
}
