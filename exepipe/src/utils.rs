use byteorder::{BigEndian, ByteOrder};
use revm::primitives::{AccountInfo, Address, FixedBytes, KECCAK_EMPTY, U256};

use crate::exetask::ACC_AND_IDX_LEN;

pub fn addr_to_u256(addr: &Address) -> U256 {
    U256::from_be_slice(&addr[..])
}

pub fn decode_account_info(bz: &[u8; ACC_AND_IDX_LEN]) -> AccountInfo {
    AccountInfo {
        balance: U256::from_be_slice(&bz[..32]),
        nonce: BigEndian::read_u64(&bz[32..32 + 8]),
        code_hash: FixedBytes::<32>::from_slice(&bz[32 + 8..ACC_AND_IDX_LEN]),
        code: None,
    }
}

pub fn is_empty_code_hash(code_hash: &FixedBytes<32>) -> bool {
    code_hash == &FixedBytes::<32>::ZERO || code_hash == &KECCAK_EMPTY
}
