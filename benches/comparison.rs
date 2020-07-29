use async_walk::WalkBuilder;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use tokio::runtime;
use walkdir::WalkDir;

const TEST_DIR: &'static str = "../../../";

// TODO: Generate benchmark directory
fn async_walk(c: &mut Criterion) {
    let mut rt = runtime::Builder::new()
        .threaded_scheduler()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("walks");
    group.sample_size(10);
    group.bench_function("async_walk", |b| {
        b.iter(|| {
            rt.block_on(async {
                WalkBuilder::new(TEST_DIR)
                    .follow_symlinks(true)
                    .build()
                    .for_each(|_| async {})
                    .await
            })
        });
    });

    group.bench_function("walkdir", |b| {
        b.iter(|| rt.block_on(async { for _entry in WalkDir::new(TEST_DIR).follow_links(true) {} }))
    });
    group.finish();
}

criterion_group!(benches, async_walk);
criterion_main!(benches);
