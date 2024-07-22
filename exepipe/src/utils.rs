use byteorder::{BigEndian, ByteOrder};
use revm::primitives::{AccountInfo, Address, FixedBytes, KECCAK_EMPTY, U256};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::exetask::ACC_INFO_LEN;

pub fn addr_to_u256(addr: &Address) -> U256 {
    U256::from_be_slice(&addr[..])
}

pub fn decode_account_info(bz: &[u8; ACC_INFO_LEN]) -> AccountInfo {
    AccountInfo {
        balance: U256::from_be_slice(&bz[..32]),
        nonce: BigEndian::read_u64(&bz[32..32 + 8]),
        code_hash: FixedBytes::<32>::from_slice(&bz[32 + 8..ACC_INFO_LEN]),
        code: None,
    }
}

pub fn is_empty_code_hash(code_hash: &FixedBytes<32>) -> bool {
    code_hash == &FixedBytes::<32>::ZERO || code_hash == &KECCAK_EMPTY
}

struct AtomicU256 {
    limbs: [AtomicU64; 4],
}

impl AtomicU256 {
    pub fn zero() -> AtomicU256 {
        let limbs: [AtomicU64; 4] = [
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
            AtomicU64::new(0),
        ];
        AtomicU256 { limbs }
    }

    pub fn to_u256(&self) -> U256 {
        let limbs: [u64; 4] = [
            self.limbs[0].load(Ordering::SeqCst),
            self.limbs[1].load(Ordering::SeqCst),
            self.limbs[2].load(Ordering::SeqCst),
            self.limbs[3].load(Ordering::SeqCst),
        ];
        U256::from_limbs(limbs)
    }

    pub fn add(&self, other: &U256) {
        let mut limbs = other.as_limbs().clone();
        let old0 = self.limbs[0].fetch_add(limbs[0], Ordering::SeqCst);
        if old0.checked_add(limbs[0]).is_none() {
            // has carry bit for higher limbs
            if let Some(x) = limbs[1].checked_add(1) {
                limbs[1] = x;
            } else {
                limbs[1] = 0;
                if let Some(x) = limbs[2].checked_add(1) {
                    limbs[2] = x;
                } else {
                    limbs[2] = 0;
                    limbs[3] += 1;
                }
            }
        }

        if limbs[1] != 0 {
            let old1 = self.limbs[1].fetch_add(limbs[1], Ordering::SeqCst);
            if old1.checked_add(limbs[1]).is_none() {
                if let Some(x) = limbs[2].checked_add(1) {
                    limbs[2] = x;
                } else {
                    limbs[2] = 0;
                    limbs[3] += 1;
                }
            }
        }
        if limbs[2] != 0 {
            let old2 = self.limbs[2].fetch_add(limbs[2], Ordering::SeqCst);
            if old2.checked_add(limbs[2]).is_none() {
                limbs[3] += 1;
            }
        }
        if limbs[3] != 0 {
            let old3 = self.limbs[3].fetch_add(limbs[3], Ordering::SeqCst);
            if old3.checked_add(limbs[3]).is_none() {
                panic!("Add Overflow");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Sub;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::thread::{self, JoinHandle};
    use std::time::Instant;

    use revm::primitives::bitvec::view::BitViewSized;
    use revm::primitives::U256;

    use crate::utils::AtomicU256;

    const N_THREADS: u64 = 10000;
    const N_TIMES: u64 = 10;

    #[test]
    fn test_atomicu256() {
        let r = Arc::new(AtomicU256::zero());
        fn add_n_times(r: Arc<AtomicU256>, n: u64) -> JoinHandle<()> {
            let _r = r.clone();
            thread::spawn(move || {
                for _ in 0..n {
                    _r.add(&U256::from(1));
                }
            })
        }
        let s = Instant::now();
        let mut threads = Vec::with_capacity(N_THREADS as usize);
        for _ in 0..N_THREADS {
            threads.push(add_n_times(r.clone(), N_TIMES));
        }

        let arr = [
            U256::from(u64::MAX),
            U256::from(u128::MAX),
            U256::from_str("7810129792545240005436734052837676976501871962928563653135").unwrap(), //u196::MAX
        ];
        for i in 0..3 {
            let _r = r.clone();
            let amount = arr[i];
            threads.push(thread::spawn(move || {
                _r.add(&U256::from(amount));
            }))
        }
        for thread in threads {
            thread.join().unwrap();
        }

        let sum: U256 = arr.iter().cloned().sum();
        assert_eq!(
            sum.wrapping_add(U256::from(N_TIMES * (N_THREADS as u64))),
            r.to_u256()
        );
        println!("{:?}", s.elapsed());
    }

    #[test]
    #[should_panic(expected = "Add Overflow")]
    fn test_atomicu256_panic() {
        let a = AtomicU256::zero();
        a.add(
            &U256::from_str(
                "115792089237316195423570985008687907853269984665640564039457584007913129639935",
            )
            .unwrap(),
        );
        a.add(&U256::from(1));
    }

    #[test]
    fn test_atomic() {
        let r = Arc::new(AtomicU64::new(0));

        fn add_n_times(r: Arc<AtomicU64>, n: u64) -> JoinHandle<()> {
            let _r = r.clone();
            thread::spawn(move || {
                for _ in 0..n {
                    _r.fetch_add(1, Ordering::SeqCst);
                }
            })
        }

        let s = Instant::now();
        let mut threads = Vec::with_capacity(N_THREADS as usize);
        for _ in 0..N_THREADS {
            threads.push(add_n_times(r.clone(), N_TIMES));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(N_TIMES * N_THREADS, r.load(Ordering::SeqCst));
        println!("{:?}", s.elapsed());
    }

    #[test]
    fn test_mutex() {
        let r = Arc::new(Mutex::new(0));

        fn add_n_times(r: Arc<Mutex<u64>>, n: u64) -> JoinHandle<()> {
            let _r = r.clone();
            thread::spawn(move || {
                for _ in 0..n {
                    let mut num = _r.lock().unwrap();
                    *num += 1;
                }
            })
        }

        let s = Instant::now();
        let mut threads = Vec::with_capacity(N_THREADS as usize);
        for _ in 0..N_THREADS {
            threads.push(add_n_times(r.clone(), N_TIMES));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(N_TIMES * (N_THREADS as u64), *r.lock().unwrap());
        println!("{:?}", s.elapsed());
    }

    #[test]
    fn test_rwlock() {
        let r = Arc::new(RwLock::new(0));

        fn add_n_times(r: Arc<RwLock<u64>>, n: u64) -> JoinHandle<()> {
            let _r = r.clone();
            thread::spawn(move || {
                for _ in 0..n {
                    let mut num = _r.write().unwrap();
                    *num += 1;
                }
            })
        }

        let s = Instant::now();
        let mut threads = Vec::with_capacity(N_THREADS as usize);
        for _ in 0..N_THREADS {
            threads.push(add_n_times(r.clone(), N_TIMES));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(N_TIMES * (N_THREADS as u64), *r.read().unwrap());
        println!("{:?}", s.elapsed());
    }
}
