mod tests {
    use mpads::{test_helper::TempDir, AdsCore};
    use seqads::SeqAds;

    const WRITE_BUF_SIZE: usize = 8 * 1024;
    const FILE_SEGMENT_SIZE: usize = 64 * 1024;

    #[test]
    fn test_xxx() {
        let dir = "./test_xxx";
        let _tmp_dir = TempDir::new(dir);

        AdsCore::init_dir(dir, FILE_SEGMENT_SIZE);
        let seq_ads = SeqAds::new(dir, WRITE_BUF_SIZE, FILE_SEGMENT_SIZE);
    }
}
