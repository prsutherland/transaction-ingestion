use rust_decimal::Decimal;
use serde::{Serialize, Serializer, ser::SerializeStruct};
use std::{
    collections::{
        HashMap,
        hash_map::Entry::{Occupied, Vacant},
    },
    error::Error,
};

use crate::err_msg;

#[derive(PartialEq, Eq)]
pub enum DisputedState {
    NotDisputed,
    Disputed,
    ChargedBack,
}

pub struct TransactionState {
    pub tx: u32,
    pub amount: Decimal,
    pub is_deposit: bool,
    pub disputed_state: DisputedState,
}

pub struct Account {
    pub client: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub locked: bool,
    pub transactions: HashMap<u32, TransactionState>,
}

impl Account {
    /// Keep the authoritative account state local to a single aggregate so transaction
    /// policies are enforced consistently regardless of the call site.
    pub fn new(client: u16) -> Self {
        Self {
            client,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            locked: false,
            transactions: HashMap::new(),
        }
    }

    pub fn deposit(&mut self, tx: u32, amount: Decimal) -> Result<(), Box<dyn Error>> {
        if amount.is_sign_negative() {
            Err(err_msg("amount cannot be negative"))
        } else if self.locked {
            Err(err_msg("account is locked"))
        } else {
            match self.transactions.entry(tx) {
                Occupied(_) => Err(err_msg("transaction already exists")),
                Vacant(entry) => {
                    entry.insert(TransactionState {
                        tx,
                        amount,
                        is_deposit: true,
                        disputed_state: DisputedState::NotDisputed,
                    });
                    self.available += amount;
                    Ok(())
                }
            }
        }
    }

    pub fn withdraw(&mut self, tx: u32, amount: Decimal) -> Result<(), Box<dyn Error>> {
        if amount.is_sign_negative() {
            Err(err_msg("amount cannot be negative"))
        } else if self.locked {
            Err(err_msg("account is locked"))
        } else if self.available < amount {
            Err(err_msg("insufficient funds"))
        } else {
            match self.transactions.entry(tx) {
                Occupied(_) => Err(err_msg("transaction already exists")),
                Vacant(entry) => {
                    entry.insert(TransactionState {
                        tx,
                        amount,
                        is_deposit: false,
                        disputed_state: DisputedState::NotDisputed,
                    });
                    self.available -= amount;
                    Ok(())
                }
            }
        }
    }

    pub fn dispute(&mut self, tx: u32) -> Result<(), Box<dyn Error>> {
        let amount = self.update_tx_with(tx, |transaction| {
            if transaction.disputed_state != DisputedState::NotDisputed {
                Err(err_msg("transaction already disputed"))?
            }
            transaction.disputed_state = DisputedState::Disputed;
            Ok(())
        })?;
        // Disputes quarantine funds by moving them to held balance until resolved or charged back.
        // If a withdrawal previously happened, available can be negative.
        self.held += amount;
        self.available -= amount;
        Ok(())
    }

    pub fn resolve(&mut self, tx: u32) -> Result<(), Box<dyn Error>> {
        let amount = self.update_tx_with(tx, |transaction| {
            if transaction.disputed_state == DisputedState::NotDisputed {
                Err(err_msg("transaction not disputed"))?
            }
            if transaction.disputed_state == DisputedState::ChargedBack {
                Err(err_msg("transaction already charged back"))?
            }
            transaction.disputed_state = DisputedState::NotDisputed;
            Ok(())
        })?;
        // Resolve returns quarantined funds to available, reversing a prior dispute hold.
        self.held -= amount;
        self.available += amount;
        Ok(())
    }

    pub fn chargeback(&mut self, tx: u32) -> Result<(), Box<dyn Error>> {
        let amount = self.update_tx_with(tx, |transaction| {
            if transaction.disputed_state != DisputedState::Disputed {
                Err(err_msg("transaction not disputed"))?
            }
            transaction.disputed_state = DisputedState::ChargedBack;
            Ok(())
        })?;
        // Chargeback permanently removes held funds from the account and freezes future
        // balance-changing activity. Available is intentionally unchanged because disputed
        // funds already moved out of available during dispute.
        //
        // If a withdrawal previously happened, available can be negative.
        self.held -= amount;
        self.locked = true;
        Ok(())
    }

    fn update_tx_with(
        &mut self,
        tx: u32,
        f: impl FnOnce(&mut TransactionState) -> Result<(), Box<dyn Error>>,
    ) -> Result<Decimal, Box<dyn Error>> {
        let transaction = self.transactions.entry(tx);
        match transaction {
            Occupied(mut entry) => {
                let transaction = entry.get_mut();
                // Business policy: only deposits can be disputed because they represent
                // inbound funds that can be clawed back by external systems.
                //
                // Theoretically, disputing a withdrawal would require our system to
                // claw back which is a separate mechanism.
                if !transaction.is_deposit {
                    Err(err_msg("transaction is not a deposit"))
                } else {
                    f(transaction)?;
                    Ok(transaction.amount)
                }
            }
            Vacant(_) => Err(err_msg("transaction not found")),
        }
    }
}

impl Serialize for Account {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Account", 5)?;
        state.serialize_field("client", &self.client)?;
        state.serialize_field("available", &self.available.normalize())?;
        state.serialize_field("held", &self.held.normalize())?;
        state.serialize_field("total", &(self.available + self.held).normalize())?;
        state.serialize_field("locked", &self.locked)?;
        state.end()
    }
}
