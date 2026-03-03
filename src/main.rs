use std::{
    error::Error,
    fs::File,
    io::{self, BufReader, Read},
};
use transaction_ingestion::{
    engine::Engine,
    transaction::parse_transaction_record,
};

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
        .flexible(true)
        .from_reader(reader);

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

    let writer = io::stdout();
    let mut csv_writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(writer);
    csv_writer.write_record(["client", "available", "held", "total", "locked"])?;

    engine.to_csv(&mut csv_writer)?;

    Ok(())
}
