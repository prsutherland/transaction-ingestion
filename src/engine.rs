use std::{
    collections::{HashMap, hash_map::Entry},
    error::Error,
    io::Write,
};

use crate::{
    account::Account,
    err_msg,
    transaction::{TransactionRecord, TransactionType},
};

#[derive(Default)]
pub struct Engine {
    lookup: HashMap<u16, Account>,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            lookup: HashMap::new(),
        }
    }

    pub fn process_transaction(
        &mut self,
        record: &TransactionRecord,
    ) -> Result<(), Box<dyn Error>> {
        let entry = self.lookup.entry(record.client);
        let inserted_new = matches!(entry, Entry::Vacant(_));
        let account = entry.or_insert_with(|| Account::new(record.client));

        let result = match record.tx_type {
            TransactionType::Deposit => {
                if let Some(amount) = record.amount {
                    account.deposit(record.tx, amount)
                } else {
                    Err(err_msg("amount is required"))?
                }
            }
            TransactionType::Withdrawal => {
                if let Some(amount) = record.amount {
                    account.withdraw(record.tx, amount)
                } else {
                    Err(err_msg("amount is required"))?
                }
            }
            TransactionType::Dispute => account.dispute(record.tx),
            TransactionType::Resolve => account.resolve(record.tx),
            TransactionType::Chargeback => account.chargeback(record.tx),
        };

        // Roll back first-touch account creation when a non-funding operation fails
        // (for example dispute on missing transaction). This avoids ghost accounts in output.
        if result.is_err() && inserted_new {
            self.lookup.remove(&record.client);
        }

        result
    }

    pub fn to_csv<W: Write>(&self, csv_writer: &mut csv::Writer<W>) -> Result<(), Box<dyn Error>> {
        // Account iteration order is intentionally unspecified (HashMap-backed) to keep
        // ingestion operations O(1) and avoid extra sort work in the hot path.
        for account in self.lookup.values() {
            csv_writer.serialize(account)?;
        }
        csv_writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::TransactionRecord;
    use crate::transaction::TransactionType;
    use rust_decimal::Decimal;
    use std::io::Cursor;

    fn record(
        client: u16,
        tx: u32,
        tx_type: TransactionType,
        amount: Decimal,
    ) -> TransactionRecord {
        TransactionRecord {
            client,
            tx,
            tx_type,
            amount: Some(amount),
        }
    }

    fn csv_output(engine: &Engine) -> csv::Reader<Cursor<Vec<u8>>> {
        let mut buffer = Vec::new();
        let mut csv_writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut buffer);
        engine.to_csv(&mut csv_writer).unwrap();
        drop(csv_writer);
        let output = String::from_utf8(buffer).unwrap();
        csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(output.as_bytes().into()))
    }

    #[test]
    fn csv_empty_when_no_accounts_exist() {
        let engine = Engine::new();

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn deposit_and_withdrawal_updates_balances() {
        let mut engine = Engine::new();

        engine
            .process_transaction(&record(
                1,
                1,
                TransactionType::Deposit,
                Decimal::TEN,
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                1,
                2,
                TransactionType::Withdrawal,
                Decimal::from_str_exact("3.5").unwrap(),
            ))
            .unwrap();

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["1", "6.5", "0", "6.5", "false"])
        );
    }

    #[test]
    fn dispute_of_existing_deposit_moves_funds_to_held() {
        let mut engine = Engine::new();

        engine
            .process_transaction(&record(
                2,
                1,
                TransactionType::Deposit,
                Decimal::from_str_exact("12.3456").unwrap(),
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                2,
                1,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap();

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["2", "0", "12.3456", "12.3456", "false"])
        );
    }

    #[test]
    fn cannot_dispute_same_transaction_twice_concurrently() {
        let mut engine = Engine::new();

        engine
            .process_transaction(&record(
                3,
                1,
                TransactionType::Deposit,
                Decimal::from_str_exact("4.25").unwrap(),
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                3,
                1,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap();

        let err = engine
            .process_transaction(&record(
                3,
                1,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap_err();
        assert_eq!(err.to_string(), "transaction already disputed");

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["3", "0", "4.25", "4.25", "false"])
        );
    }

    #[test]
    fn multiple_transactions_can_be_disputed_simultaneously() {
        let mut engine = Engine::new();

        engine
            .process_transaction(&record(
                4,
                1,
                TransactionType::Deposit,
                Decimal::from_str_exact("3.5").unwrap(),
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                4,
                2,
                TransactionType::Deposit,
                Decimal::from_str_exact("1.25").unwrap(),
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                4,
                1,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                4,
                2,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap();

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["4", "0", "4.75", "4.75", "false"])
        );
    }

    #[test]
    fn transaction_can_be_redisputed_after_resolve() {
        // Assumption: resolve returns a transaction to a state where future disputes are legal.
        let mut engine = Engine::new();

        engine
            .process_transaction(&record(
                5,
                1,
                TransactionType::Deposit,
                Decimal::from_str_exact("2.75").unwrap(),
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                5,
                1,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                5,
                1,
                TransactionType::Resolve,
                Decimal::ZERO,
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                5,
                1,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap();

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["5", "0", "2.75", "2.75", "false"])
        );
    }

    #[test]
    fn locked_account_rejects_deposits() {
        // Policy: lock blocks new money movement into the account.
        let mut engine = Engine::new();
        let account = engine.lookup.entry(6).or_insert_with(|| Account::new(6));
        account.locked = true;

        let err = engine
            .process_transaction(&record(
                6,
                2,
                TransactionType::Deposit,
                Decimal::ONE,
            ))
            .unwrap_err();
        assert_eq!(err.to_string(), "account is locked");

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["6", "0", "0", "0", "true"])
        );
    }

    #[test]
    fn locked_account_rejects_withdrawals() {
        // Policy: lock blocks new money movement out of the account.
        let mut engine = Engine::new();
        let account = engine.lookup.entry(7).or_insert_with(|| Account::new(7));
        account.locked = true;

        let err = engine
            .process_transaction(&record(
                7,
                1,
                TransactionType::Withdrawal,
                Decimal::ONE,
            ))
            .unwrap_err();
        assert_eq!(err.to_string(), "account is locked");

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["7", "0", "0", "0", "true"])
        );
    }

    #[test]
    fn dispute_resolve_and_chargeback_can_process_on_locked_account() {
        // Policy: lock does not block dispute lifecycle so historical fraud workflows can complete.
        let mut engine = Engine::new();

        engine
            .process_transaction(&record(
                8,
                1,
                TransactionType::Deposit,
                Decimal::from_str_exact("5").unwrap(),
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                8,
                2,
                TransactionType::Deposit,
                Decimal::from_str_exact("7").unwrap(),
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                8,
                3,
                TransactionType::Deposit,
                Decimal::from_str_exact("11").unwrap(),
            ))
            .unwrap();

        let account = engine.lookup.entry(8).or_insert_with(|| Account::new(8));
        account.locked = true;

        engine
            .process_transaction(&record(
                8,
                1,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                8,
                2,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                8,
                1,
                TransactionType::Resolve,
                Decimal::ZERO,
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                8,
                2,
                TransactionType::Chargeback,
                Decimal::ZERO,
            ))
            .unwrap();

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["8", "16", "0", "16", "true"])
        );
    }

    #[test]
    fn withdrawal_fails_when_funds_are_insufficient() {
        let mut engine = Engine::new();

        engine
            .process_transaction(&record(
                9,
                1,
                TransactionType::Deposit,
                Decimal::ONE,
            ))
            .unwrap();
        let err = engine
            .process_transaction(&record(
                9,
                2,
                TransactionType::Withdrawal,
                Decimal::TWO,
            ))
            .unwrap_err();
        assert_eq!(err.to_string(), "insufficient funds");
    }

    #[test]
    fn negative_deposit_amount_is_rejected() {
        let mut engine = Engine::new();
        let err = engine
            .process_transaction(&record(
                10,
                1,
                TransactionType::Deposit,
                Decimal::NEGATIVE_ONE,
            ))
            .unwrap_err();

        assert_eq!(err.to_string(), "amount cannot be negative");
    }

    #[test]
    fn negative_withdrawal_amount_is_rejected() {
        let mut engine = Engine::new();

        engine
            .process_transaction(&record(
                11,
                1,
                TransactionType::Deposit,
                Decimal::TWO,
            ))
            .unwrap();

        let err = engine
            .process_transaction(&record(
                11,
                2,
                TransactionType::Withdrawal,
                Decimal::NEGATIVE_ONE,
            ))
            .unwrap_err();
        assert_eq!(err.to_string(), "amount cannot be negative");

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["11", "2", "0", "2", "false"])
        );
    }

    #[test]
    fn dispute_requires_deposit_transaction_kind() {
        let mut engine = Engine::new();

        engine
            .process_transaction(&record(
                12,
                1,
                TransactionType::Deposit,
                Decimal::from_str_exact("5").unwrap(),
            ))
            .unwrap();
        engine
            .process_transaction(&record(
                12,
                2,
                TransactionType::Withdrawal,
                Decimal::ONE,
            ))
            .unwrap();

        let err = engine
            .process_transaction(&record(
                12,
                2,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap_err();
        assert_eq!(err.to_string(), "transaction is not a deposit");

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0],
            &csv::StringRecord::from(vec!["12", "4", "0", "4", "false"])
        );
    }

    #[test]
    fn failed_dispute_does_not_create_account() {
        // Assumption: invalid lifecycle events should not materialize empty accounts.
        let mut engine = Engine::new();
        let err = engine
            .process_transaction(&record(
                13,
                99,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap_err();
        assert_eq!(err.to_string(), "transaction not found");

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();
        assert!(rows.is_empty());
    }

    #[test]
    fn failed_resolve_does_not_create_account() {
        // Assumption: invalid lifecycle events should not materialize empty accounts.
        let mut engine = Engine::new();
        let err = engine
            .process_transaction(&record(
                14,
                99,
                TransactionType::Resolve,
                Decimal::ZERO,
            ))
            .unwrap_err();
        assert_eq!(err.to_string(), "transaction not found");

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();
        assert!(rows.is_empty());
    }

    #[test]
    fn failed_chargeback_does_not_create_account() {
        // Assumption: invalid lifecycle events should not materialize empty accounts.
        let mut engine = Engine::new();
        let err = engine
            .process_transaction(&record(
                15,
                99,
                TransactionType::Chargeback,
                Decimal::ZERO,
            ))
            .unwrap_err();
        assert_eq!(err.to_string(), "transaction not found");

        let mut reader = csv_output(&engine);
        let rows: Vec<csv::StringRecord> = reader.records().map(|row| row.unwrap()).collect();
        assert!(rows.is_empty());
    }

    #[test]
    fn dispute_requires_existing_deposit_transaction() {
        let mut engine = Engine::new();
        let err = engine
            .process_transaction(&record(
                10,
                99,
                TransactionType::Dispute,
                Decimal::ZERO,
            ))
            .unwrap_err();

        assert_eq!(err.to_string(), "transaction not found");
    }
}
