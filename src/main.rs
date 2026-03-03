mod transaction;

use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, Read};
use crate::transaction::TransactionRecord;

fn process_transaction(record: TransactionRecord) {
    let TransactionRecord {
        client,
        tx,
        tx_type,
        amount,
    } = record;

    let _ = (client, tx, tx_type, amount);
}

fn input_reader() -> Result<Box<dyn Read>, Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    if let Some(path) = args.next() {
        let file = File::open(path)?;
        return Ok(Box::new(BufReader::new(file)));
    }

    Ok(Box::new(io::stdin().lock()))
}

fn main() -> Result<(), Box<dyn Error>> {
    let reader = input_reader()?;

    let mut csv_reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(reader);

    for result in csv_reader.deserialize::<TransactionRecord>() {
        let record = result?;
        process_transaction(record);
    }

    Ok(())
}
