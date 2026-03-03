use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize)]
pub struct TransactionRecord {
    pub client: u16,
    pub tx: u32,
    #[serde(rename = "type")]
    pub tx_type: TransactionType,
    #[serde(deserialize_with = "deserialize_amount_4dp")]
    pub amount: Decimal,
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