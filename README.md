# transaction-ingestion

Streaming transaction ingestion engine for deposits, withdrawals, disputes, resolves, and chargebacks.

## What It Does

- Ingests CSV transactions from a file path or `stdin`.
- Applies account mutations in arrival order.
- Supports dispute lifecycle transitions on deposit transactions.
- Emits final account balances as CSV:
  - `client`
  - `available`
  - `held`
  - `total`
  - `locked`

## Running

```bash
cargo run -- path/to/input.csv
```

or:

```bash
cat path/to/input.csv | cargo run
```

## Testing And Benchmarks

```bash
cargo test
cargo bench
```

Integration tests use fixtures from `test_inputs`.

## Architectural Decisions

### 1) Ledger + State Model Per Account

Each account keeps:
- aggregate balances (`available`, `held`, `locked`)
- a per-transaction ledger (`transactions`) with dispute state

Why:
- Disputes are keyed to historical transactions, so aggregate balances alone are insufficient.
- Storing transaction-level state makes dispute/resolve/chargeback transitions explicit and auditable.
- Withdrawls are not disputable, but need to be kept to flag duplicate transactions.

### 2) Exact Decimal Arithmetic

`rust_decimal` is used for all money values.

Why:
- Binary floating-point is unsafe for financial reconciliation.
- Decimal arithmetic preserves expected financial precision and reporting consistency.

### 3) Parse Fast Path For Hot Loop

The runtime path in `main` uses `parse_transaction_record` over `csv::ByteRecord` instead of serde-per-row decode.

Why:
- Ingest throughput is dominated by parse + apply.
- Hand-rolled parsing reduces per-row overhead and improves benchmarked throughput in this codebase.

### 4) Fail-Open Ingestion

Invalid input rows are logged to `stderr` and skipped; ingestion continues.

Why:
- Batch financial feeds often contain a minority of malformed lines.
- Stopping on first bad record increases operational risk and replay cost for otherwise valid batches.

### 5) Anti-Ghost Account Creation

`Engine::process_transaction` lazily creates accounts, but removes newly created accounts if the first operation fails.

Why:
- Prevents empty/phantom accounts from appearing in output due to invalid dispute/resolve/chargeback events.
- Keeps outputs semantically aligned with successful account activity.

## Design Assumptions And Invariants

- Transaction ids (`tx`) are unique per account across all transaction kinds.
- Dispute/resolve/chargeback only apply to deposit transactions.
- A locked account rejects new deposits/withdrawals.
- Dispute lifecycle operations are still allowed on locked accounts so historical cases can be finalized.
- Output row order is not guaranteed (accounts are stored in a `HashMap`).
- Withdrawls and deposits with a zero amount are allowed.

## Project Layout

- `src/main.rs`: CLI entrypoint, CSV ingest loop, and output.
- `src/engine.rs`: transaction dispatch and account registry.
- `src/account.rs`: account state machine and dispute lifecycle.
- `src/transaction.rs`: transaction model and parsing.
- `tests/main_all_inputs.rs`: binary-level integration tests.
- `benches/`: Criterion throughput benchmarks.
- `benches/upper_bound_bench.rs`: Shows theoretical maximum performance.

## Future Considerations

### Parallelism

The workload is trivially parallelizable by account. An `Engine` object can
be created per-thread and only the account id parsed with:

```rust
str::from_utf8(bytes)?.parse::<u16>()?
```

It can be routed to the correct thread using

```rust
fn hash(account: u16) -> u16 {
  account % THREADS
}
```

Once complete, `main.rs` can call `engine.to_csv()` on all `Engine` instances.
This is why `main.rs` writes the headers and not `Engine`.

### Memory usage vs persistance

To avoid holding all transactions in memory, the internals of `Engine` can
be replaced with database queries. This is largely why most transaction
handling tests are on `Engine` and not `Account`.

I prototyped this with in memory DuckDB and found performance dropped to
~600 rows/second. An OLAP database was likely the wrong choice and an OLTP
database would be more performant.

A more performant approach would likely be to organize multiple servers in a
ring and all incoming transactions are routed to the right nodes. Each node
would agressively cache what it could in RAM in the existing `Engine` data
structure. Only when a dispute/resolve/chargeback occurs would it query on
disk data.
