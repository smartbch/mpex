# Analyze the inter-dependency of transactions

## In-order serial transaction execution

To consider WETH and use only one set for readonly and read-n-write data:

```
cargo run --features uni_set,U64
```

To consider WETH and use two seperate sets for readonly and read-n-write data:

```
cargo run --features U64
```

To ignore WETH and use only one set for readonly and read-n-write data:

```
cargo run --features ignore_weth,uni_set,U64
```

To ignore WETH and use two seperate sets for readonly and read-n-write data:

```
cargo run --features ignore_weth,U64
```

## Out-of-order execution with schedulers

To reorder the transactions, we have two kinds of schedulers: the list of transaction sets and the bloom filter. Both of them use two seperate sets for readonly and read-n-write data.

Use a list of 64 transaction sets for reordering:

```
cargo run --features reorder,ignore_weth,U64
```

Use 64 bloom filters for reordering:

```
cargo run --features reorder,ignore_weth,bloomfilter,U64
```

If you want to change the number from 64 to 32 or 128, just change the "U64" feature to "U32" or "U128".

## Test the bloomfilter-based scheduler's speed

We recommend to use the bloomfilter-based scheduler in production, because it is the most efficient. To test its speed, use the following command:

```
cargo run --release --features reorder,ignore_weth,bloomfilter,U64,test_speed
```

