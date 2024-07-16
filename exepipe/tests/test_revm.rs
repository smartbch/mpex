#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, str::FromStr, sync::Arc};

    use revm::{
        db::{states::cache, CacheDB, EmptyDB},
        handler::register::{self, EvmHandler, HandleRegisterBox},
        primitives::{
            address, alloy_primitives::TxKind, keccak256, AccountInfo, Address, BlockEnv, Bytes,
            ByzantiumSpec, CancunSpec, CfgEnv, EVMError, HandlerCfg, ResultAndState, SpecId,
            TransactTo, KECCAK_EMPTY, U256,
        },
        Database, Evm, Handler,
    };

    #[test]
    fn test_new_eoa_code_hash() {
        let a1 = address!("00000000000000000000000000000000000000a1");
        let a2 = address!("00000000000000000000000000000000000000a2");
        let a3 = address!("00000000000000000000000000000000000000a3");

        let one_ether = U256::from(1_000_000_000_000_000_000u128);
        let a1_info = AccountInfo {
            nonce: 0_u64,
            balance: one_ether,
            code_hash: keccak256(Bytes::new()),
            code: None,
        };

        let mut cache_db = CacheDB::new(EmptyDB::default());
        cache_db.insert_account_info(a1, a1_info);

        let mut evm = Evm::builder()
            .with_ref_db(cache_db)
            .modify_tx_env(|tx| {
                tx.caller = a1.clone();
                tx.transact_to = TransactTo::Call(a2);
                tx.data = Bytes::new();
                tx.value = U256::from(123456789);
            })
            .build();

        evm.transact().unwrap();
        evm.transact_commit().unwrap();

        assert_eq!(
            KECCAK_EMPTY.to_string(),
            evm.db_mut()
                .basic(a1)
                .unwrap()
                .unwrap()
                .code_hash
                .to_string()
        );
        // new EOA created by REVM
        assert_eq!(
            KECCAK_EMPTY.to_string(),
            evm.db_mut()
                .basic(a2)
                .unwrap()
                .unwrap()
                .code_hash
                .to_string()
        );
        assert_eq!(Option::None, evm.db_mut().basic(a3).unwrap());
    }

    #[test]
    fn test_handler() {
        let register = |inner: &Rc<RefCell<i32>>| -> HandleRegisterBox<(), EmptyDB> {
            let inner = inner.clone();
            Box::new(move |h| {
                *inner.borrow_mut() += 1;
                h.post_execution.output = Arc::new(|_, _| Err(EVMError::Custom("test".to_string())))
            })
        };

        let mut handler = EvmHandler::<(), EmptyDB>::new(HandlerCfg::new(SpecId::LATEST));
        let test = Rc::new(RefCell::new(0));

        handler.append_handler_register_box(register(&test));
        assert_eq!(*test.borrow(), 1);
    }
}
