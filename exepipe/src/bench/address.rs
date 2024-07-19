use revm::primitives::{alloy_primitives::U160, Address};

pub fn create_caller_address(index: usize /* start with 1 */) -> Address {
    if index == 0 {
        panic!("index should start with 1");
    }
    Address::from(U160::from(index))
}

pub fn create_to_address(index: u64) -> Address {
    if index == 0 {
        panic!("index should start with 1");
    }
    Address::from_slice(&U160::from(index).as_le_slice())
}
