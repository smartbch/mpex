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
        let limbs: [AtomicU64; 4] = [AtomicU64::new(0), 
            AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
        AtomicU256 { limbs, }
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
        if old0 > limbs[0] { // has carry bit for higher limbs
            limbs[1] += 1;
            if limbs[1] == 0 {
                limbs[2] += 1;
                if limbs[2] == 0 {
                    limbs[3] += 1;
                }
            }
        }
        if limbs[1] != 0 {
            let old1 = self.limbs[1].fetch_add(limbs[1], Ordering::SeqCst);
            if old1 > limbs[1] {
                limbs[2] += 1;
                if limbs[2] == 0 {
                    limbs[3] += 1;
                }
            }
        }
        if limbs[2] != 0 {
            let old2 = self.limbs[2].fetch_add(limbs[2], Ordering::SeqCst);
            if old2 > limbs[2] {
                limbs[3] += 1;
            }
        }
        if limbs[3] != 0 {
            self.limbs[3].fetch_add(limbs[3], Ordering::SeqCst);
        }
    }
}
