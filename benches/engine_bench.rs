use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rust_decimal::Decimal;
use transaction_ingestion::{
    engine::Engine,
    transaction::{TransactionRecord, TransactionType},
};

fn record(client: u16, tx: u32, tx_type: TransactionType, amount: Decimal) -> TransactionRecord {
    TransactionRecord {
        client,
        tx,
        tx_type,
        amount: Some(amount),
    }
}

fn bench_engine_transaction_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_transaction_throughput");
    let iterations = 100_000u32;
    group.throughput(Throughput::Elements((iterations * 2) as u64));

    group.bench_with_input(
        BenchmarkId::new("deposit_withdrawal_loop", iterations),
        &iterations,
        |b, &iterations| {
            b.iter(|| {
                let mut engine = Engine::new();
                let client = 1u16;

                for i in 0..iterations {
                    let deposit_tx_id = (i * 2) + 1;
                    engine
                        .process_transaction(&record(
                            client,
                            deposit_tx_id,
                            TransactionType::Deposit,
                            Decimal::from_str_exact("1.0000").unwrap(),
                        ))
                        .unwrap();
                    engine
                        .process_transaction(&record(
                            client,
                            deposit_tx_id + 1,
                            TransactionType::Withdrawal,
                            Decimal::from_str_exact("0.1000").unwrap(),
                        ))
                        .unwrap();
                }
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_engine_transaction_throughput);
criterion_main!(benches);
