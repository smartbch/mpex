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

## Schedulers reorder transactions

We have two kinds of schedulers: the list of transaction sets and the bloom filter. Both of them use two seperate sets for readonly and read-n-write data.

Use a list of 64 transaction sets for reordering:

```
cargo run --features reorder,ignore_weth,U64
```

Use 64 bloom filters for reordering:

```
cargo run --features reorder,ignore_weth,bloomfilter,U64
```

If you want to change the number from 64 to 32 or 128, just change the "U64" feature to "U32" or "U128".

