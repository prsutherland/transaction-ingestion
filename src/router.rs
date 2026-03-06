use crossbeam_channel::{Receiver, Sender, bounded};
use std::error::Error;
use std::io::{self, BufRead, BufReader};
use std::fs::File;
use std::thread;

use crate::engine::Engine;
use crate::reader::CsvU16RowReader;
use crate::transaction::{TransactionRecord, parse_transaction_row_bytes};


pub fn run(reader: BufReader<File>, workers: usize) -> Result<(), Box<dyn Error>> {
    run_inner(reader, workers, true)
}

pub fn run_without_output(reader: BufReader<File>, workers: usize) -> Result<(), Box<dyn Error>> {
    run_inner(reader, workers, false)
}

pub fn run_reader_without_output<R: BufRead>(
    reader: R,
    workers: usize,
) -> Result<(), Box<dyn Error>> {
    run_inner(reader, workers, false)
}

fn run_inner<R: BufRead>(reader: R, workers: usize, write_output: bool) -> Result<(), Box<dyn Error>> {
    assert!(workers > 0);

    // One channel per worker: avoids contention from many consumers on one queue.
    let mut txs: Vec<Sender<TransactionRecord>> = Vec::with_capacity(workers);
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::with_capacity(workers);

    for _ in 0..workers {
        let (tx, rx) = bounded::<TransactionRecord>(8192);
        txs.push(tx);
        handles.push(thread::spawn(move || worker(rx, write_output)));
    }

    let mut it = CsvU16RowReader::new(reader);
    while let Some((id, row_bytes)) = it.next()? {
        let record = match parse_transaction_row_bytes(row_bytes) {
            Ok(record) => record,
            Err(e) => {
                eprintln!("error parsing transaction: {}", e);
                continue;
            }
        };

        let shard = (id as usize) % workers;
        txs[shard].send(record).expect("worker hung up");
    }

    if write_output {
        // Write headers
        let writer = io::stdout();
        let mut csv_writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(writer);
        csv_writer.write_record(["client", "available", "held", "total", "locked"])?;
        csv_writer.flush()?;
    }

    // Close channels (which will signal workers to write their results) and join workers.
    for (tx, h) in txs.into_iter().zip(handles.into_iter()) {
        drop(tx);
        let _ = h.join().expect("worker panicked");
    }

    Ok(())
}

fn worker(rx: Receiver<TransactionRecord>, write_output: bool) {
    let mut engine = Engine::new();

    while let Ok(record) = rx.recv() {
        if let Err(e) = engine.process_transaction(&record) {
            eprintln!(
                "error processing account={} transaction={}: {}",
                record.client, record.tx, e
            );
        }
    }

    if write_output {
        // Write results to stdout
        let mut csv_writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(io::stdout());
        engine.to_csv(&mut csv_writer).unwrap();
        csv_writer.flush().unwrap();
    }
}
