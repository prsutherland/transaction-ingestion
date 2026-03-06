use std::io::Cursor;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use transaction_ingestion::reader::CsvU16RowReader;

fn build_csv_input(iterations: u32) -> Vec<u8> {
    let mut csv_input = String::from("type,client,tx,amount\n");

    for i in 0..iterations {
        let deposit_tx_id = (i * 2) + 1;
        csv_input.push_str(&format!("deposit,1,{deposit_tx_id},1.0000\n"));
        csv_input.push_str(&format!("withdrawal,1,{},0.1000\n", deposit_tx_id + 1));
    }

    csv_input.into_bytes()
}

fn bench_csv_parse_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_parse_throughput");
    let iterations = 100_000u32;
    group.throughput(Throughput::Elements((iterations * 2) as u64));

    let csv_input = build_csv_input(iterations);

    group.bench_with_input(
        BenchmarkId::new("csv_parse_only", iterations),
        &iterations,
        |b, _| {
            b.iter(|| {
                let mut csv_reader = CsvU16RowReader::new(Cursor::new(&csv_input));

                let mut total = 0;
                while let Some(_) = csv_reader.next().expect("failed to parse row") {
                    total += 1;
                }
                total
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_csv_parse_throughput);
criterion_main!(benches);
