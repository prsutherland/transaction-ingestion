use num_cpus;
use std::error::Error;
use transaction_ingestion::input_path;
use transaction_ingestion::router;

fn main() -> Result<(), Box<dyn Error>> {
    let path = input_path()?;
    let workers = num_cpus::get();
    router::run(path.as_path(), workers)?;
    Ok(())
}
