use std::collections::HashMap;

use anyhow::{anyhow, Result};
use bincode;
use mpads::changeset::ChangeSet;
use mpads::entry::{EntryBz, Hash32};
use mpads::multiproof::Witness;
use mpads::utils::hasher;
use revm::db::Database;
use revm::precompile::primitives::{AccountInfo, Bytecode, B256, U256};
use revm::precompile::Address;
use revm::primitives::FixedBytes;

use crate::exetask::{ExeTask, ACC_INFO_LEN};
use crate::utils::{decode_account_info, is_empty_code_hash, join_address_index};

struct ProvingCtx {
    entry_map: HashMap<Hash32, Vec<u8>>,
}

impl ProvingCtx {
    fn new(entries: &Vec<EntryBz>) -> Self {
        let mut entry_map = HashMap::new();
        for entry in entries {
            entry_map.insert(entry.hash(), entry.bz.to_vec());
        }

        ProvingCtx { entry_map }
    }
}

impl Database for ProvingCtx {
    type Error = anyhow::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let key_hash = hasher::hash(&address[..]);
        if let Some(bz) = self.entry_map.get(&key_hash) {
            let entry_bz = EntryBz { bz };

            let mut buf = [0u8; ACC_INFO_LEN];
            buf[..ACC_INFO_LEN].copy_from_slice(entry_bz.value());

            Ok(Some(decode_account_info(&buf)))
        } else {
            Err(anyhow!("Account {} not found!", address))
        }
    }

    fn code_by_hash(&mut self, code_hash: FixedBytes<32>) -> Result<Bytecode, Self::Error> {
        if is_empty_code_hash(&code_hash) {
            return Ok(Bytecode::new());
        }

        if let Some(bz) = self.entry_map.get(&code_hash[..]) {
            let bc: Option<Bytecode> = bincode::deserialize(bz).unwrap();
            Ok(bc.unwrap())
        } else {
            Err(anyhow!("Code {} not found!", code_hash))
        }
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let addr_idx = join_address_index(&address, &index);
        let key_hash = hasher::hash(&addr_idx[..]);
        if let Some(bz) = self.entry_map.get(&key_hash[..]) {
            let entry_bz = EntryBz { bz };
            Ok(U256::from_be_slice(entry_bz.value()))
        } else {
            Ok(U256::ZERO)
        }
    }

    fn block_hash(&mut self, _number: U256) -> Result<B256, Self::Error> {
        Ok(FixedBytes::<32>::ZERO)
    }
}

fn validate(task: ExeTask, witness: Witness, entries: Vec<Vec<u8>>) {}

fn verify_entries(witness: Witness, entries: Vec<Vec<u8>>) {}

fn exec_task(task: ExeTask, entries: Vec<Vec<u8>>) {}

fn apply_change_set(witness: Witness, change_set: ChangeSet) {}

fn verify_witness(witness: Witness) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xxx() {
        let entries = vec![];
        let ctx = ProvingCtx::new(&entries);
    }
}
