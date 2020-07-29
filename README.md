[![crates.io](https://img.shields.io/crates/v/async-walk.svg)](https://crates.io/crates/async-walk)
[![docs.rs](https://docs.rs/async-walk/badge.svg)](https://docs.rs/async-walk/)

# async-walk

A concurrent fs walk library built on top of tokio.

## Usage

```rust
use async_walk::WalkBuilder;
let walk = WalkBuilder::new(TEST_DIR).build();
walk.for_each(|entry| async {
    println!("{:?}", entry.path());
});
```

## License

`async-walk` is currently licensed under the MIT license. See LICENSE for more details.
