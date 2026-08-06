use crate::models::{Block, Event, NewAccountActivity, Transaction};
use chrono::DateTime;
use serde_json::json;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxFee {
    pub amount: String,
    pub payer: String,
}

#[derive(Debug, Default)]
pub struct ActivityContext {
    /// request_key -> fee
    pub tx_fees: HashMap<String, TxFee>,
    /// block hash -> miner account, decoded from the payload's `minerData`
    pub miners_by_block: HashMap<String, String>,
}

impl ActivityContext {
    pub fn new(txs: &[Transaction], miners_by_block: HashMap<String, String>) -> Self {
        let tx_fees = txs
            .iter()
            .map(|tx| {
                (
                    tx.request_key.clone(),
                    TxFee {
                        amount: compute_fee(tx.gas, tx.gas_price),
                        payer: tx.sender.clone(),
                    },
                )
            })
            .collect();

        Self {
            tx_fees,
            miners_by_block,
        }
    }

    fn is_gas_payment(&self, block: &str, request_key: &str, from: &str, to: &str) -> bool {
        let (Some(miner), Some(fee)) = (
            self.miners_by_block.get(block),
            self.tx_fees.get(request_key),
        ) else {
            return false;
        };

        to == miner && from == fee.payer
    }
}

pub fn compute_fee(gas: i64, gas_price: f64) -> String {
    (gas.max(0) as f64 * gas_price.max(0.0)).to_string()
}

/// Attaches `fee`/`fee_payer` to one activity per (transaction, account).
///
/// A transaction pays a single fee but can emit several transfers to the same account.
/// Repeating the fee on each would make one charge look like several to anything summing a
/// feed, so it gets attached on the lowest `idx` only.
fn attach_fees(activities: &mut [NewAccountActivity], context: &ActivityContext) {
    let mut lowest_idx: HashMap<(&str, &str), usize> = HashMap::new();

    for (position, activity) in activities.iter().enumerate() {
        let key = (activity.request_key.as_str(), activity.account.as_str());
        match lowest_idx.get(&key) {
            Some(&existing) if activities[existing].idx <= activity.idx => {}
            _ => {
                lowest_idx.insert(key, position);
            }
        }
    }

    let targets: Vec<usize> = lowest_idx.into_values().collect();
    for position in targets {
        let Some(fee) = context.tx_fees.get(&activities[position].request_key) else {
            continue;
        };
        let (amount, payer) = (fee.amount.clone(), fee.payer.clone());

        if let Some(details) = activities[position].details.as_object_mut() {
            details.insert("fee".to_string(), json!(amount));
            details.insert("fee_payer".to_string(), json!(payer));
        }
    }
}

/// Parse transfer events into account activities
/// Creates two activities per transfer: one for sender, one for receiver
pub fn parse_transfer_activities(
    events: &[Event],
    blocks: &[Block],
    context: &ActivityContext,
) -> Vec<NewAccountActivity> {
    let blocks_by_hash = blocks
        .iter()
        .map(|block| (block.hash.to_string(), block))
        .collect::<HashMap<String, &Block>>();

    events
        .iter()
        .filter(|event| event.name == "TRANSFER")
        .flat_map(|event| {
            let block = match blocks_by_hash.get(&event.block) {
                Some(b) => b,
                None => return vec![],
            };

            // Parse transfer parameters (indexed by position like in make_transfer)
            let from_account = event.params[0].as_str().unwrap_or("").to_string();
            let to_account = event.params[1].as_str().unwrap_or("").to_string();

            // The gas settlement is surfaced as `fee` on the transfer it paid for, not as an
            // activity of its own.
            if context.is_gas_payment(&event.block, &event.request_key, &from_account, &to_account)
            {
                return vec![];
            }

            // Parse amount - same logic as make_transfer
            let amount = if event.params[2].is_number() {
                event.params[2].to_string()
            } else if event.params[2].is_object() {
                let obj = event.params[2].as_object().unwrap();
                if let Some(decimal) = obj.get("decimal") {
                    decimal.as_str().unwrap_or("0").to_string()
                } else if let Some(int_val) = obj.get("int") {
                    int_val.as_i64().unwrap_or(0).to_string()
                } else {
                    "0".to_string()
                }
            } else {
                "0".to_string()
            };

            let creation_time =
                DateTime::from_timestamp_micros(block.creation_time.and_utc().timestamp_micros())
                    .unwrap()
                    .naive_utc();

            let mut activities = Vec::new();

            // Activity for sender (if not empty)
            if !from_account.is_empty() {
                activities.push(NewAccountActivity {
                    account: from_account.clone(),
                    activity_type: "TRANSFER".to_string(),
                    module_name: event.module.clone(),
                    chain_id: event.chain_id,
                    height: event.height,
                    block: event.block.clone(),
                    request_key: event.request_key.clone(),
                    creation_time,
                    pact_id: event.pact_id.clone(),
                    idx: event.idx,
                    details: json!({
                        "direction": "sent",
                        "from": from_account,
                        "to": to_account,
                        "amount": amount,
                    }),
                });
            }

            // Activity for receiver (if not empty)
            if !to_account.is_empty() {
                activities.push(NewAccountActivity {
                    account: to_account.clone(),
                    activity_type: "TRANSFER".to_string(),
                    module_name: event.module.clone(),
                    chain_id: event.chain_id,
                    height: event.height,
                    block: event.block.clone(),
                    request_key: event.request_key.clone(),
                    creation_time,
                    pact_id: event.pact_id.clone(),
                    idx: event.idx,
                    details: json!({
                        "direction": "received",
                        "from": from_account,
                        "to": to_account,
                        "amount": amount,
                    }),
                });
            }

            activities
        })
        .collect()
}

/// Main function to process all account activities from events
pub fn process_account_activities(
    events: &[Event],
    blocks: &[Block],
    context: &ActivityContext,
) -> Vec<NewAccountActivity> {
    let mut activities = Vec::new();

    // Process transfers
    activities.extend(parse_transfer_activities(events, blocks, context));

    // Future: Add more activity types here
    // activities.extend(parse_swap_activities(events, blocks));
    // kaddex.exchange ["coin:runonflux.flux", {"decimal": "966820.886762665755"}, 93691.18039296]
    // kdlaunch.kdswap-exchange ["coin:n_582fed11af00dc626812cd7890bb88e72067f28c.bro", {"decimal": "24274.320638269222"}, 0.335216976685]
    // activities.extend(parse_liquidity_activities(events, blocks));

    attach_fees(&mut activities, context);

    activities
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;

    const MINER: &str = "k:e7f7130f359fb1f8c87873bf858a0e9cbc3c1059f62ae715ec72e760b055e9f3";
    const GAS_STATION: &str = "k:93face383ca119d68ab1cb21de1a711bd00b8e4bd47e717fceb0e78329027407";
    const SENDER: &str = "k:7be65ff759b17d601183de1ea8cbdb004d21bf2b4f29b005c83ca7911832f39b";
    const RECIPIENT: &str = "k:3787e2f95b566189b6c0b53777f9aa1696370375e3c6a9e96892811c089e1992";
    const BLOCK: &str = "wM_Cex67dqvZuUOkWgEDkWvuF9pIX-Fu8lKcT2zZQCM";
    const REQUEST_KEY: &str = "rmUfB60xA5MBBwI-Z94CdEQ8k7wSI4bY4qjx721JJsM";

    fn timestamp() -> NaiveDateTime {
        NaiveDateTime::parse_from_str("2026-06-27T12:45:38.730935", "%Y-%m-%dT%H:%M:%S%.f").unwrap()
    }

    fn block() -> Block {
        Block {
            chain_id: 2,
            creation_time: timestamp(),
            hash: BLOCK.to_string(),
            height: 7001001,
            parent: "parent".to_string(),
        }
    }

    fn transfer_event(idx: i64, from: &str, to: &str, amount: f64) -> Event {
        Event {
            block: BLOCK.to_string(),
            chain_id: 2,
            height: 7001001,
            idx,
            module: "coin".to_string(),
            module_hash: "hash".to_string(),
            name: "TRANSFER".to_string(),
            params: json!([from, to, amount]),
            param_text: String::new(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: REQUEST_KEY.to_string(),
            pact_id: None,
        }
    }

    fn transaction(gas: i64, gas_price: f64, sender: &str) -> Transaction {
        Transaction {
            bad_result: None,
            block: BLOCK.to_string(),
            chain_id: 2,
            continuation: None,
            creation_time: timestamp(),
            gas,
            gas_limit: 1000,
            gas_price,
            good_result: None,
            height: 7001001,
            logs: None,
            metadata: None,
            nonce: "nonce".to_string(),
            pact_id: None,
            request_key: REQUEST_KEY.to_string(),
            rollback: None,
            sender: sender.to_string(),
            step: None,
            ttl: 28800,
            tx_id: None,
        }
    }

    fn context(sender: &str) -> ActivityContext {
        ActivityContext::new(
            &[transaction(224, 1e-8, sender)],
            HashMap::from([(BLOCK.to_string(), MINER.to_string())]),
        )
    }

    #[test]
    fn the_gas_transfer_does_not_become_an_activity() {
        let events = vec![
            transfer_event(0, SENDER, RECIPIENT, 0.1),
            // The gas settlement, paid by the gas station to the miner.
            transfer_event(1, GAS_STATION, MINER, 2.24e-06),
        ];

        let activities = process_account_activities(&events, &[block()], &context(GAS_STATION));

        // Two rows for the real transfer (sender + recipient), and nothing for gas.
        assert_eq!(activities.len(), 2);
        let accounts: Vec<&str> = activities.iter().map(|a| a.account.as_str()).collect();
        assert!(accounts.contains(&SENDER));
        assert!(accounts.contains(&RECIPIENT));
        assert!(
            !accounts.contains(&MINER),
            "the miner must not receive a dust activity"
        );
    }

    #[test]
    fn fee_and_payer_are_attached_to_the_transfer() {
        let events = vec![
            transfer_event(0, SENDER, RECIPIENT, 0.1),
            transfer_event(1, GAS_STATION, MINER, 2.24e-06),
        ];

        let activities = process_account_activities(&events, &[block()], &context(GAS_STATION));

        for activity in &activities {
            assert_eq!(activity.details["fee"], json!("0.00000224"));
            assert_eq!(activity.details["fee_payer"], json!(GAS_STATION));
            assert_ne!(activity.details["fee_payer"], json!(SENDER));
        }
    }

    #[test]
    fn a_self_paid_fee_names_the_sender_as_payer() {
        let events = vec![
            transfer_event(0, SENDER, RECIPIENT, 0.1),
            transfer_event(1, SENDER, MINER, 2.24e-06),
        ];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));

        assert_eq!(activities.len(), 2, "gas still filtered when self-paid");
        let sent = activities
            .iter()
            .find(|a| a.account == SENDER)
            .expect("sender row");
        assert_eq!(sent.details["fee_payer"], json!(SENDER));
    }

    #[test]
    fn one_fee_per_account_even_with_several_transfers() {
        let events = vec![
            transfer_event(0, SENDER, RECIPIENT, 0.1),
            transfer_event(1, SENDER, RECIPIENT, 0.2),
            transfer_event(2, SENDER, MINER, 2.24e-06),
        ];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));

        for account in [SENDER, RECIPIENT] {
            let with_fee = activities
                .iter()
                .filter(|a| a.account == account && a.details.get("fee").is_some())
                .collect::<Vec<_>>();

            assert_eq!(with_fee.len(), 1, "{account} should carry the fee once");
            assert_eq!(with_fee[0].idx, 0, "it belongs on the lowest idx");
        }
    }

    #[test]
    fn a_transfer_to_the_miner_from_someone_else_is_kept() {
        let events = vec![transfer_event(0, RECIPIENT, MINER, 5.0)];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));

        assert_eq!(activities.len(), 2, "a genuine transfer to the miner stays");
    }
}
