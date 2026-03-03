use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader, Read};

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize)]
struct TransactionRecord {
    client: u16,
    tx: u32,
    #[serde(rename = "type")]
    tx_type: TransactionType,
    #[serde(deserialize_with = "deserialize_amount_4dp")]
    amount: Decimal,
}

fn deserialize_amount_4dp<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    let amount = raw.parse::<Decimal>().map_err(serde::de::Error::custom)?;

    if amount.scale() > 4 {
        return Err(serde::de::Error::custom(
            "amount must have at most 4 digits after the decimal point",
        ));
    }

    Ok(amount)
}

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
