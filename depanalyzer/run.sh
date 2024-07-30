cargo run --release --features uni_set,U64 &> a1
cargo run --release --features U64 &> a2
cargo run --release --features ignore_weth,uni_set,U64 &> a3
cargo run --release --features ignore_weth,U64 &> a4

cargo run --release --features reorder,ignore_weth,U32 &> s32
cargo run --release --features reorder,ignore_weth,bloomfilter,U32 &> b32

cargo run --release --features reorder,ignore_weth,U64 &> s64
cargo run --release --features reorder,ignore_weth,bloomfilter,U64 &> b64

cargo run --release --features reorder,ignore_weth,U128 &> s128
cargo run --release --features reorder,ignore_weth,bloomfilter,U128 &> b128

