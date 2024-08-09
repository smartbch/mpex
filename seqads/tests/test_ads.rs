
#[cfg(test)]
mod tests {
    use mpads::AdsCore;
    use mpads::changeset::ChangeSet;
    use mpads::def::OP_CREATE;
    use mpads::refdb::OpRecord;
    use mpads::utils::hasher;
    use seqads::{SeqAds, SeqAdsWrap};
    #[test]
    fn test_seqads() {
        let ads_dir = "./SEQADS";
        let wrbuf_size = 8 * 1024 * 1024;
        let file_block_size = 1024 * 1024 * 1024;
        AdsCore::init_dir(ads_dir, file_block_size);
        let mut ads = SeqAds::new(ads_dir, wrbuf_size, file_block_size);
        let mut change_set = ChangeSet::new();
        let address = [1;20];
        let key = hasher::hash(address);
        let shard_id = key[0] >> 4;
        change_set.add_op(
            OP_CREATE,
            shard_id,
            &key,       //used during sort as key_hash
        &address[..], //the key
        &[2;20], //the value
            None,
        );
        change_set.sort();
        let change_sets = vec![change_set];
        let nums = ads.indexer.len(shard_id as usize);
        println!("{}", nums);
        ads.commit_tx(1, &change_sets);
        let nums = ads.indexer.len(shard_id as usize);
        println!("{}", nums);
    }
}