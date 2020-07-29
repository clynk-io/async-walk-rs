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
