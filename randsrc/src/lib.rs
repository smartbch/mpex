extern crate core;

use std::fs::{File};
use std::io::{Read, Seek, SeekFrom};
use blake2::{Blake2b512, Digest};
use byteorder::{ByteOrder, LittleEndian};
use std::str;
pub struct RandSrc {
    file: File,
    hasher: Blake2b512,
    buf: Vec<u8>,
    idx: i64,
    last_rand: [u8; 64],
}

impl RandSrc {
    pub fn new(file_name: &str, seed: &str) -> RandSrc {
        let file = File::open(file_name).unwrap();
        let mut h = Blake2b512::new();
        h.update(seed.as_bytes());
        let mut rs = RandSrc {
            file,
            hasher: Blake2b512::new(),
            buf: vec![],
            idx: -1,
            last_rand: h.finalize().into(),
        };
        rs.step();
        return rs;
    }

    fn new512bits(&mut self) -> Vec<u8> {
        let mut buf = [0u8; 32];
        let n = self.file.read(&mut buf[..]).unwrap();
        if n == 0 {
            self.file.seek(SeekFrom::Start(0)).unwrap();
            self.file.read(&mut buf[..]).unwrap();
        }
        let mut hasher = self.hasher.clone();
        hasher.update(&self.last_rand[..]);
        hasher.update(&buf[..]);
        let res = hasher.finalize();
        self.last_rand.copy_from_slice(&res[..]);
        return res[..].to_owned();
    }


    fn step(&mut self) {
        let mut arr_a = [[0u8; 64]; 16];
        let mut arr_b = [[0u8; 64]; 16];
        for i in 0..16 {
            arr_a[i] = <[u8; 64]>::try_from(self.new512bits()).unwrap();
            arr_b[i] = <[u8; 64]>::try_from(self.new512bits()).unwrap();
        };
        self.buf.clear();
        for i in 0..16 {
            for j in 0..16 {
                let mut buf = [0u8; 64];
                buf[..].copy_from_slice(&arr_a[i]);
                for k in 0..16 {
                    buf[k] = buf[k] ^ arr_b[j][k];
                }
                self.buf.extend_from_slice(&buf[..]);
            }
        }
        self.idx = 0;
    }

    pub fn fill_bytes(&mut self, bz: &mut [u8]) {
        for i in 0..bz.len() {
            bz[i] = self.buf[self.idx as usize];
            self.idx += 1;
            if self.idx as usize == self.buf.len() {
                self.step();
            }
        }
    }

    pub fn get_bytes(&mut self, n: usize) -> Vec<u8> {
        let mut res = Vec::with_capacity(n);
        while res.len() < n {
            res.push(self.buf[self.idx as usize]);
            self.idx += 1;
            if self.idx as usize == self.buf.len() {
                self.step();
            }
        }
        return res;
    }

    pub fn get_string(&mut self, n :usize) -> String {
        let chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".as_bytes();
        let mut res = Vec::with_capacity(n);
        let bz = self.get_bytes(n);
        for b in bz.iter() {
            let j = *b as usize % chars.len();
            res.push(chars[j]);
        };
        str::from_utf8(res.as_slice()).unwrap().into()
    }

    pub fn get_bool(&mut self) -> bool {
        let bz = self.get_bytes(1);
        return bz[0] != 0;
    }

    pub fn get_uint8(&mut self) -> u8 {
        let bz = self.get_bytes(1);
        return bz[0];
    }

    pub fn get_uint16(&mut self) -> u16 {
        let bz = self.get_bytes(2);
        return LittleEndian::read_u16(bz.as_slice());
    }

    pub fn get_uint32(&mut self) -> u32 {
        let bz = self.get_bytes(4);
        return LittleEndian::read_u32(bz.as_slice());
    }

    pub fn get_uint64(&mut self) -> u64 {
        let bz = self.get_bytes(8);
        return LittleEndian::read_u64(bz.as_slice());
    }

    pub fn get_int8(&mut self) -> i8 {
        self.get_uint8() as i8
    }

    pub fn get_int16(&mut self) -> i16 {
        self.get_uint16() as i16
    }

    pub fn get_int32(&mut self) -> i32 {
        self.get_uint32() as i32
    }

    pub fn get_int64(&mut self) -> i64 {
        self.get_uint64() as i64
    }

    pub fn get_float32(&mut self) -> f32 {
        self.get_uint32() as f32
    }

    pub fn get_float64(&mut self) -> f64 {
        self.get_uint64() as f64
    }
}

#[cfg(test)]
mod test_rand_src {
    use std::fs::{File, remove_file};
    use std::io::Write;
    use crate::RandSrc;

    #[test]
    fn test() {
        let file_name = "test_rand_src.txt";
        let mut f = File::create(file_name).unwrap();
        let mut content = [2u8;128];
        for i in 0..128 {
            content[i] = i as u8;
        }
        f.write(content.as_slice()).unwrap();
        let mut rs = RandSrc::new(file_name, "hello");
        let data = rs.get_uint32();
        println!("{}", data);
        let data = rs.get_uint32();
        println!("{}", data);
        let data = rs.get_uint32();
        println!("{}", data);
        let data = rs.get_uint8();
        println!("{}", data);
        let data = rs.get_uint8();
        println!("{}", data);
        let data = rs.get_uint8();
        println!("{}", data);
        let data = rs.get_string(10);
        println!("{}", data);
        let data = rs.get_string(10);
        println!("{}", data);
        let data = rs.get_string(10);
        println!("{}", data);
        remove_file(file_name).unwrap();
    }

}
