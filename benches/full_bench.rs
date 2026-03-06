use std::io::Cursor;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use transaction_ingestion::router;

fn build_csv_input(iterations: u32) -> Vec<u8> {
    let mut csv_input = String::from("type,client,tx,amount\n");

    for i in 0..iterations {
        let deposit_tx_id = (i * 2) + 1;
        csv_input.push_str(&format!("deposit,{},{deposit_tx_id},1.0000\n", i%100));
        csv_input.push_str(&format!("withdrawal,{},{},0.1000\n", i%100, deposit_tx_id + 1));
    }

    csv_input.into_bytes()
}

fn bench_full_transaction_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_transaction_throughput");
    let iterations = 100_000u32;
    group.throughput(Throughput::Elements((iterations * 2) as u64));

    let csv_input = build_csv_input(iterations);
    let workers = 2;

    group.bench_with_input(
        BenchmarkId::new("router_run", iterations),
        &iterations,
        |b, _| {
            b.iter(|| {
                let reader = Cursor::new(&csv_input);
                router::run_reader_without_output(reader, workers).expect("router benchmark failed");
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_full_transaction_throughput);
criterion_main!(benches);
