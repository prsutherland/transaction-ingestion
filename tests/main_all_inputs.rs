use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

fn binary_path() -> PathBuf {
    let deps_dir = std::env::current_exe().unwrap();
    let debug_dir = deps_dir.parent().unwrap().parent().unwrap();

    let binary_names = ["transaction-ingestion", "transaction_ingestion"];
    binary_names
        .iter()
        .map(|name| debug_dir.join(name))
        .find(|path| path.exists())
        .expect("transaction-ingestion binary should be present in target/debug")
}

fn parse_output_rows(stdout: &[u8]) -> Vec<csv::StringRecord> {
    let mut reader = csv::Reader::from_reader(stdout);
    assert_eq!(
        reader.headers().unwrap(),
        &csv::StringRecord::from(vec!["client", "available", "held", "total", "locked"])
    );

    let mut rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();
    // The binary emits accounts from a HashMap-backed engine; sort here so assertions
    // encode business output, not incidental map iteration order.
    rows.sort_by_key(|row| row.get(0).unwrap().parse::<u16>().unwrap());
    rows
}

fn run_input_file(path: &Path) -> (Vec<csv::StringRecord>, String) {
    let output = Command::new(binary_path())
        .arg(path)
        .output()
        .expect("main binary should run");

    assert!(
        output.status.success(),
        "binary failed for {}: {}",
        path.display(),
        String::from_utf8_lossy(&output.stderr)
    );

    (
        parse_output_rows(&output.stdout),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

fn input_path(file_name: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_inputs")
        .join(file_name);
    assert!(path.exists(), "missing test input file: {}", path.display());
    path
}

#[test]
fn main_processes_basic_csv() {
    let (rows, stderr) = run_input_file(&input_path("basic.csv"));

    assert_eq!(
        rows,
        vec![
            csv::StringRecord::from(vec!["1", "1.5", "0", "1.5", "false"]),
            csv::StringRecord::from(vec!["2", "2", "0", "2", "false"]),
        ]
    );
    // Design assumption: ingestion is fail-open. Invalid rows are reported to stderr
    // while valid rows still contribute to final balances.
    assert!(stderr.contains("insufficient funds"));
}

#[test]
fn main_processes_dispute_csv() {
    let (rows, stderr) = run_input_file(&input_path("dispute.csv"));

    assert_eq!(
        rows,
        vec![
            csv::StringRecord::from(vec!["1", "0.5", "0", "0.5", "true"]),
            csv::StringRecord::from(vec!["2", "2", "0", "2", "false"]),
        ]
    );
    // Design assumption: same fail-open behavior applies during dispute lifecycle handling.
    assert!(stderr.contains("insufficient funds"));
}

#[test]
fn main_processes_invalid_amounts_csv() {
    let (rows, stderr) = run_input_file(&input_path("invalid_amounts.csv"));

    // Assumption: parse-layer amount validation rejects all rows before account creation.
    assert!(rows.is_empty());
    assert!(stderr.contains("invalid amount"));
}
