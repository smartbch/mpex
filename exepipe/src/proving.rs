use std::collections::HashMap;

use anyhow::{anyhow, Result};
use bincode;
use mpads::changeset::ChangeSet;
use mpads::entry::{EntryBz, Hash32};
use mpads::multiproof::{verify_entries, verify_witness, Witness};
use mpads::utils::hasher;
use revm::db::Database;
use revm::precompile::primitives::{AccountInfo, Bytecode, B256, U256};
use revm::precompile::Address;
use revm::primitives::{BlockEnv, CfgEnv, EVMError, Env, FixedBytes, ResultAndState, TxEnv};
use revm::Evm;

use crate::context::create_mpex_handler;
use crate::exetask::{
    get_change_set_and_check_access_rw, AccInfo, AccessSet, ExeTask, ACC_INFO_LEN,
};
use crate::statecache::StateCache;
use crate::utils::{decode_account_info, is_empty_code_hash, join_address_index};

#[derive(Clone)]
struct ProvingCtx {
    orig_acc_map: HashMap<Address, AccInfo>,
    entry_map: HashMap<Hash32, Vec<u8>>,
    code_map: HashMap<B256, Bytecode>,
}

impl ProvingCtx {
    fn new(entries: &Vec<EntryBz>, codes: &Vec<Bytecode>) -> Self {
        let mut entry_map = HashMap::new();
        let mut code_map = HashMap::new();
        for entry in entries {
            entry_map.insert(entry.key_hash(), entry.bz.to_vec());
        }
        for code in codes {
            code_map.insert(get_code_hash(code), code.clone());
        }

        ProvingCtx {
            orig_acc_map: HashMap::new(),
            entry_map,
            code_map,
        }
    }
}

fn get_code_hash(code: &Bytecode) -> B256 {
    let code_opt = Option::Some(code.clone());
    let code_data = bincode::serialize(&code_opt).unwrap();
    B256::from(hasher::hash(&code_data))
}

impl Database for ProvingCtx {
    type Error = anyhow::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let key_hash = hasher::hash(&address[..]);
        if let Some(bz) = self.entry_map.get(&key_hash) {
            let entry_bz = EntryBz { bz };

            let mut buf = [0u8; ACC_INFO_LEN];
            buf[..ACC_INFO_LEN].copy_from_slice(entry_bz.value());

            self.orig_acc_map.insert(address, buf);
            Ok(Some(decode_account_info(&buf)))
        } else {
            Err(anyhow!("Account {} not found!", address))
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if is_empty_code_hash(&code_hash) {
            return Ok(Bytecode::new());
        }

        if let Some(code) = self.code_map.get(&code_hash[..]) {
            Ok(code.clone())
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

struct Validator<'a> {
    tx: &'a TxEnv,
    codes: &'a Vec<Bytecode>,
    entries: &'a Vec<EntryBz<'a>>,
    leaf_offsets: &'a Vec<(usize, usize)>,
    witness: &'a Witness,
    start_sn_for_new_entry: u64,
    old_root: &'a Hash32,
    new_root: &'a Hash32,
}

impl<'a> Validator<'a> {
    fn new(
        tx: &'a TxEnv,
        codes: &'a Vec<Bytecode>,
        entries: &'a Vec<EntryBz>,
        leaf_offsets: &'a Vec<(usize, usize)>,
        witness: &'a Witness,
        start_sn_for_new_entry: u64,
        old_root: &'a Hash32,
        new_root: &'a Hash32,
    ) -> Self {
        Self {
            tx,
            codes,
            entries,
            leaf_offsets,
            witness,
            start_sn_for_new_entry,
            old_root,
            new_root,
        }
    }

    fn validate(&self) {
        let ok = verify_entries(
            self.start_sn_for_new_entry,
            self.entries,
            self.leaf_offsets,
            self.witness,
        );
        let (rs, cs) = self.exec_tx();
        self.fill_new_value(&cs); // TODO
        let ok = verify_witness(self.witness, self.old_root, self.new_root);
    }

    fn exec_tx(&self) -> (Result<ResultAndState>, ChangeSet) {
        let db = ProvingCtx::new(self.entries, self.codes);

        let env = Box::new(Env {
            cfg: CfgEnv::default(),
            block: BlockEnv::default(), // TODO
            tx: self.tx.clone(),
        });

        let handler = create_mpex_handler::<(), ProvingCtx>(0);
        let mut evm = Evm::builder()
            .with_db(db.clone())
            .with_env(env)
            .with_handler(handler)
            .build();
        let evm_result = evm.transact();
        if let Err(err) = evm_result {
            return (
                Err(anyhow!("EVM transact error: {:?}", err)),
                ChangeSet::new(),
            );
        }

        let res_and_state = evm_result.unwrap();
        let get_cs_result = get_change_set_and_check_access_rw(
            &res_and_state.state,
            &db.orig_acc_map,
            &StateCache::new(),
            &AccessSet::new(),
            false,
        );
        if let Err(err) = get_cs_result {
            return (
                Err(anyhow!("Commit state change error: {:?}", err)),
                ChangeSet::new(),
            );
        }

        return (Ok(res_and_state), get_cs_result.unwrap());
    }

    fn fill_new_value(&self, cs: &ChangeSet) {
        // TODO
    }

    // fn apply_change_set(witness: Witness, change_set: ChangeSet) {}
}

#[cfg(test)]
mod tests {
    use mpads::test_helper::EntryBuilder;
    use revm::primitives::{address, Bytes};

    use crate::test_helper::encode_account_info;

    use super::*;

    #[test]
    fn test_proving_ctx_basic() {
        let a1 = address!("0000000000000000000000000000000000000001");

        let bz1 = EntryBuilder::kv(
            a1.to_vec(),
            encode_account_info(&AccountInfo {
                balance: U256::ZERO,
                nonce: 123,
                code_hash: B256::ZERO,
                code: Option::None,
            }),
        )
        .build_and_dump(&[]);
        let entry1 = EntryBz { bz: &bz1 };
        let entries = vec![entry1];

        let mut ctx = ProvingCtx::new(&entries, &vec![]);
        let acc1 = ctx.basic(a1).unwrap().unwrap();
        assert_eq!(acc1.nonce, 123);
    }

    #[test]
    fn test_proving_ctx_storage() {
        let a1 = address!("0000000000000000000000000000000000000001");
        let slot = U256::from(12345);
        let val = U256::from(67890);
        let addr_slot = join_address_index(&a1, &slot);

        let bz1 = EntryBuilder::kv(addr_slot.to_vec(), val.to_be_bytes_vec()).build_and_dump(&[]);
        let entry1 = EntryBz { bz: &bz1 };
        let entries = vec![entry1];

        let mut ctx = ProvingCtx::new(&entries, &vec![]);
        let val2 = ctx.storage(a1, slot).unwrap();
        assert_eq!(val2, val);
    }

    #[test]
    fn test_proving_ctx_code() {
        let code = Bytecode::new_raw(Bytes::from([0x5f, 0x5f, 0x5f, 0x00]));
        let code_hash = get_code_hash(&code);
        let mut ctx = ProvingCtx::new(&vec![], &vec![code.clone()]);
        let code2 = ctx.code_by_hash(code_hash).unwrap();
        assert_eq!(code2, code);
    }
}
