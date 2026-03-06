use crossbeam_channel::{Receiver, Sender, bounded};
use std::error::Error;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Cursor};
use std::path::Path;
use std::thread;

use crate::engine::Engine;
use crate::reader::CsvU16RowReader;
use crate::transaction::{TransactionRecord, TransactionType, parse_transaction_row_bytes_into};

pub fn run(path: &Path, workers: usize) -> Result<(), Box<dyn Error>> {
    run_path_inner(path, workers, true)
}

pub fn run_reader_without_output<R: BufRead>(
    reader: R,
    workers: usize,
) -> Result<(), Box<dyn Error>> {
    run_reader_inner(reader, workers, false)
}

fn run_path_inner(path: &Path, workers: usize, write_output: bool) -> Result<(), Box<dyn Error>> {
    assert!(workers > 0);
    let mut files = Vec::with_capacity(workers);
    for _ in 0..workers {
        files.push(File::open(path)?);
    }
    run_workers(files, workers, write_output)
}

fn run_reader_inner<R: BufRead>(
    reader: R,
    workers: usize,
    write_output: bool,
) -> Result<(), Box<dyn Error>> {
    assert!(workers > 0);

    let mut raw = Vec::with_capacity(1 << 20);
    let mut reader = reader;
    reader.read_to_end(&mut raw)?;
    let shared = raw.as_slice();

    let (done_tx, done_rx) = bounded::<()>(workers);
    let mut dump_txs: Vec<Sender<()>> = Vec::with_capacity(workers);

    thread::scope(|scope| {
        for thread_idx in 0..workers {
            let worker_done_tx = done_tx.clone();
            let (dump_tx, dump_rx) = bounded::<()>(1);
            dump_txs.push(dump_tx);
            scope.spawn(move || {
                worker_loop_reader(
                    shared,
                    workers,
                    thread_idx,
                    write_output,
                    worker_done_tx,
                    dump_rx,
                )
            });
        }
        drop(done_tx);

        for _ in 0..workers {
            done_rx.recv().expect("worker hung up before completion");
        }

        if write_output {
            let writer = io::stdout();
            let mut csv_writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(writer);
            csv_writer.write_record(["client", "available", "held", "total", "locked"])?;
            csv_writer.flush()?;
        }

        for tx in dump_txs {
            tx.send(()).expect("worker hung up before dump signal");
        }

        Ok::<(), Box<dyn Error>>(())
    })?;

    Ok(())
}

fn run_workers(files: Vec<File>, workers: usize, write_output: bool) -> Result<(), Box<dyn Error>> {
    assert_eq!(files.len(), workers);
    let (done_tx, done_rx) = bounded::<()>(workers);
    let mut dump_txs: Vec<Sender<()>> = Vec::with_capacity(workers);
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::with_capacity(workers);

    for (thread_idx, file) in files.into_iter().enumerate() {
        let worker_done_tx = done_tx.clone();
        let (dump_tx, dump_rx) = bounded::<()>(1);
        dump_txs.push(dump_tx);
        handles.push(thread::spawn(move || {
            worker_loop_file(
                file,
                workers,
                thread_idx,
                write_output,
                worker_done_tx,
                dump_rx,
            )
        }));
    }
    drop(done_tx);

    for _ in 0..workers {
        done_rx.recv().expect("worker hung up before completion");
    }

    if write_output {
        let writer = io::stdout();
        let mut csv_writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(writer);
        csv_writer.write_record(["client", "available", "held", "total", "locked"])?;
        csv_writer.flush()?;
    }

    for tx in dump_txs {
        tx.send(()).expect("worker hung up before dump signal");
    }

    for h in handles {
        h.join().expect("worker panicked");
    }

    Ok(())
}

fn worker_loop_file(
    file: File,
    workers: usize,
    thread_idx: usize,
    write_output: bool,
    done_tx: Sender<()>,
    dump_rx: Receiver<()>,
) {
    let reader = BufReader::with_capacity(1 << 20, file);
    worker_loop(reader, workers, thread_idx, write_output, done_tx, dump_rx);
}

fn worker_loop_reader(
    bytes: &[u8],
    workers: usize,
    thread_idx: usize,
    write_output: bool,
    done_tx: Sender<()>,
    dump_rx: Receiver<()>,
) {
    let reader = Cursor::new(bytes);
    worker_loop(reader, workers, thread_idx, write_output, done_tx, dump_rx);
}

fn worker_loop<R: BufRead>(
    reader: R,
    workers: usize,
    thread_idx: usize,
    write_output: bool,
    done_tx: Sender<()>,
    dump_rx: Receiver<()>,
) {
    let mut engine = Engine::new();
    let mut it = CsvU16RowReader::new(reader);
    let mut record = TransactionRecord {
        client: 0,
        tx: 0,
        tx_type: TransactionType::Deposit,
        amount: None,
    };

    loop {
        let next = match it.next() {
            Ok(next) => next,
            Err(e) => {
                eprintln!("error reading transaction stream: {}", e);
                continue;
            }
        };

        let Some((id, row_bytes)) = next else {
            break;
        };

        if (id as usize) % workers != thread_idx {
            continue;
        }

        if let Err(e) = parse_transaction_row_bytes_into(row_bytes, &mut record) {
            eprintln!("error parsing transaction: {}", e);
            continue;
        }

        if let Err(e) = engine.process_transaction(&record) {
            eprintln!(
                "error processing account={} transaction={}: {}",
                record.client, record.tx, e
            );
        }
    }

    done_tx
        .send(())
        .expect("controller hung up before completion");
    dump_rx
        .recv()
        .expect("controller hung up before dump signal");

    if write_output {
        // Write results to stdout
        let mut csv_writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(io::stdout());
        engine.to_csv(&mut csv_writer).unwrap();
        csv_writer.flush().unwrap();
    }
}
