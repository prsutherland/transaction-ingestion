use std::io::Cursor;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use transaction_ingestion::{engine::Engine, transaction::parse_transaction_record};

fn build_csv_input(iterations: u32) -> Vec<u8> {
    let mut csv_input = String::from("type,client,tx,amount\n");

    for i in 0..iterations {
        let deposit_tx_id = (i * 2) + 1;
        csv_input.push_str(&format!("deposit,1,{deposit_tx_id},1.0000\n"));
        csv_input.push_str(&format!("withdrawal,1,{},0.1000\n", deposit_tx_id + 1));
    }

    csv_input.into_bytes()
}

fn bench_full_transaction_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_transaction_throughput");
    let iterations = 100_000u32;
    group.throughput(Throughput::Elements((iterations * 2) as u64));

    let csv_input = build_csv_input(iterations);

    group.bench_with_input(
        BenchmarkId::new("deposit_withdrawal_loop_with_csv_parse", iterations),
        &iterations,
        |b, _| {
            b.iter(|| {
                let mut csv_reader = csv::ReaderBuilder::new()
                    .trim(csv::Trim::All)
                    .flexible(true)
                    .from_reader(Cursor::new(&csv_input));
                let mut engine = Engine::new();

                for result in csv_reader.byte_records() {
                    let raw = match result {
                        Ok(raw) => raw,
                        Err(e) => {
                            eprintln!("error reading transaction: {}", e);
                            continue;
                        }
                    };
                    let record = match parse_transaction_record(&raw) {
                        Ok(record) => record,
                        Err(e) => {
                            eprintln!("error reading transaction: {}", e);
                            continue;
                        }
                    };

                    if let Err(e) = engine.process_transaction(&record) {
                        eprintln!(
                            "error processing account={} transaction={}: {}",
                            record.client, record.tx, e
                        );
                    }
                }
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_full_transaction_throughput);
criterion_main!(benches);
