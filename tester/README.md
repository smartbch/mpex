## Tester Collection

### The random source file

First of all, you need to copy a large file (video file, archive file, installation package, etc) into this tester directory and rename it to "randsrc.dat". It will be used as the random source.

### Fuzz Test for MPADS

To run a fuzz test for mpads, use the following command:

```
cargo run --bin v1_fuzz
```


### Speed Test for MPADS

To test the speed of mpads, make sure you have at least 16GB free memory and 80GB free SSD, and use the following command: 

```
/usr/bin/time -l cargo run --bin v2_speed --release -- 27
```

This command uses a test set with `2**27` elements. If you have 24GB free memory and 160GB free SSD, you can also use the following command:

```
/usr/bin/time -l cargo run --bin v2_speed --release -- 28
```

Just change 27 to 28. You can also try more elements by increasing the number to 29, 30, 31 or higher, if your machine has enough resource. Double resource will be needed after increasing the number by 1.

