use crate::models::{Block, Event, NewAccountActivity};
use chrono::DateTime;
use serde_json::json;
use std::collections::HashMap;

/// Parse transfer events into account activities
/// Creates two activities per transfer: one for sender, one for receiver
pub fn parse_transfer_activities(events: &[Event], blocks: &[Block]) -> Vec<NewAccountActivity> {
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
pub fn process_account_activities(events: &[Event], blocks: &[Block]) -> Vec<NewAccountActivity> {
    let mut activities = Vec::new();

    // Process transfers
    activities.extend(parse_transfer_activities(events, blocks));

    // Future: Add more activity types here
    // activities.extend(parse_swap_activities(events, blocks));
    // activities.extend(parse_liquidity_activities(events, blocks));

    activities
}
