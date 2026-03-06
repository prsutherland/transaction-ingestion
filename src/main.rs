use std::error::Error;
use transaction_ingestion::input_reader;
use transaction_ingestion::router;
use num_cpus;

fn main() -> Result<(), Box<dyn Error>> {
    let reader = input_reader()?;
    let workers = num_cpus::get();
    router::run(reader, workers)?;
    Ok(())
}
