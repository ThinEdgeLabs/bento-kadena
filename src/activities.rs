use crate::event_filter::supported_dex;
use crate::models::{Block, Event, NewAccountActivity, Transaction};
use bigdecimal::BigDecimal;
use chrono::DateTime;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;

const WALLET_SWAP_FEE_ACCOUNT_ENV: &str = "WALLET_SWAP_FEE_ACCOUNT";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxFee {
    pub amount: String,
    pub payer: String,
}

#[derive(Debug)]
pub struct ActivityContext {
    /// request_key -> fee
    pub tx_fees: HashMap<String, TxFee>,
    /// block hash -> miner account, decoded from the payload's `minerData`
    pub miners_by_block: HashMap<String, String>,
    /// Recipient used by Linx wallet swap-fee transfers.
    pub wallet_swap_fee_account: String,
}

impl ActivityContext {
    pub fn new(txs: &[Transaction], miners_by_block: HashMap<String, String>) -> Self {
        let wallet_swap_fee_account =
            env::var(WALLET_SWAP_FEE_ACCOUNT_ENV).expect("Missing WALLET_SWAP_FEE_ACCOUNT");
        Self::with_wallet_swap_fee_account(txs, miners_by_block, wallet_swap_fee_account)
    }

    pub fn with_wallet_swap_fee_account(
        txs: &[Transaction],
        miners_by_block: HashMap<String, String>,
        wallet_swap_fee_account: String,
    ) -> Self {
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
            wallet_swap_fee_account,
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

/// Attaches the network fee and payer to one activity per (transaction, account).
///
/// A transaction pays a single fee but can emit several transfers to the same account.
/// Repeating the fee on each would make one charge look like several to anything summing a
/// feed. A recognized swap owns the transaction semantics, so it is preferred over unrelated
/// transfers; transactions without a swap retain the lowest-`idx` convention.
fn attach_fees(activities: &mut [NewAccountActivity], context: &ActivityContext) {
    let mut lowest_idx: HashMap<(&str, &str), usize> = HashMap::new();

    for (position, activity) in activities.iter().enumerate() {
        let key = (activity.request_key.as_str(), activity.account.as_str());
        match lowest_idx.get(&key) {
            Some(&existing)
                if activities[existing].activity_type == "SWAP"
                    && activity.activity_type != "SWAP" => {}
            Some(&existing)
                if activities[existing].activity_type == activity.activity_type
                    && activities[existing].idx <= activity.idx => {}
            Some(&existing)
                if activities[existing].activity_type != "SWAP"
                    && activity.activity_type != "SWAP"
                    && activities[existing].idx <= activity.idx => {}
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

        let fee_field = if activities[position].activity_type == "SWAP" {
            "network_fee"
        } else {
            "fee"
        };
        if let Some(details) = activities[position].details.as_object_mut() {
            details.insert(fee_field.to_string(), json!(amount));
            details.insert("fee_payer".to_string(), json!(payer));
        }
    }
}

/// Amount as a decimal string. The chain sends it as a number or as a `{decimal}`/`{int}` object.
fn parse_amount(value: &serde_json::Value) -> String {
    if value.is_number() {
        let raw = value.to_string();
        return BigDecimal::from_str(&raw)
            .map(|amount| amount.to_plain_string())
            .unwrap_or(raw);
    }
    let Some(object) = value.as_object() else {
        return "0".to_string();
    };
    if let Some(decimal) = object.get("decimal") {
        let raw = decimal.as_str().unwrap_or("0");
        BigDecimal::from_str(raw)
            .map(|amount| amount.to_plain_string())
            .unwrap_or_else(|_| "0".to_string())
    } else if let Some(int_value) = object.get("int") {
        int_value.as_i64().unwrap_or(0).to_string()
    } else {
        "0".to_string()
    }
}

fn add_amounts(left: &str, right: &str) -> Option<String> {
    Some(
        (BigDecimal::from_str(left).ok()? + BigDecimal::from_str(right).ok()?)
            .normalized()
            .to_plain_string(),
    )
}

fn new_activity(
    event: &Event,
    block: &Block,
    account: String,
    details: serde_json::Value,
) -> NewAccountActivity {
    NewAccountActivity {
        account,
        activity_type: "TRANSFER".to_string(),
        module_name: event.module.clone(),
        chain_id: event.chain_id,
        height: event.height,
        block: event.block.clone(),
        request_key: event.request_key.clone(),
        creation_time: DateTime::from_timestamp_micros(
            block.creation_time.and_utc().timestamp_micros(),
        )
        .unwrap()
        .naive_utc(),
        pact_id: event.pact_id.clone(),
        idx: event.idx,
        details,
    }
}

/// One row for the sender, one for the receiver.
fn same_chain_activities(
    event: &Event,
    block: &Block,
    context: &ActivityContext,
) -> Vec<NewAccountActivity> {
    // Parse transfer parameters (indexed by position like in make_transfer)
    let from_account = event.params[0].as_str().unwrap_or("").to_string();
    let to_account = event.params[1].as_str().unwrap_or("").to_string();

    if context.is_gas_payment(&event.block, &event.request_key, &from_account, &to_account) {
        return vec![];
    }

    // An empty side is the burn or mint half of a cross-chain transfer. Its paired XCHAIN
    // event names the real counterparty and produces the row instead, so this one is dropped
    // rather than shown as a transfer to or from nobody.
    if from_account.is_empty() || to_account.is_empty() {
        return vec![];
    }

    let amount = parse_amount(&event.params[2]);

    vec![
        new_activity(
            event,
            block,
            from_account.clone(),
            json!({
                "direction": "sent",
                "from": from_account,
                "to": to_account,
                "amount": amount,
            }),
        ),
        new_activity(
            event,
            block,
            to_account.clone(),
            json!({
                "direction": "received",
                "from": from_account,
                "to": to_account,
                "amount": amount,
            }),
        ),
    ]
}

/// The only defpact whose `X_RESUME` arguments describe a cross-chain transfer.
const XCHAIN_DEFPACT: &str = "coin.transfer-crosschain";

/// Leg 2's sender, recovered from the `pact.X_RESUME` emitted in the same transaction.
///
/// `params` is `["<source chain>", "<defpact>", [sender, receiver, guard, target, amount]]`.
/// The argument order is inferred from `coin.transfer-crosschain`'s signature, and the one
/// mainnet sample is a self-transfer where positions 0 and 1 cannot be told apart — so the
/// receiver position must agree with the receiver `TRANSFER_XCHAIN_RECD` independently gives
/// before position 0 is trusted. `None` means fall back rather than guess.
fn recover_xchain_sender(resume: &Event, receiver: &str) -> Option<String> {
    if resume.params.get(1)?.as_str()? != XCHAIN_DEFPACT {
        return None;
    }

    let args = resume.params.get(2)?.as_array()?;
    let sender = args.first()?.as_str()?;

    if sender.is_empty() || args.get(1)?.as_str()? != receiver {
        return None;
    }

    Some(sender.to_string())
}

/// One row per leg: the sender's on leg 1, the receiver's on leg 2.
///
/// Leg 1's receiver holds nothing yet, and leg 2's mint names no sender on chain. Both ends
/// of `xchain` are filled on both legs so a client never has to work out which leg it is
/// looking at; `completed` is derived on read and deliberately not stored.
fn cross_chain_activity(
    event: &Event,
    block: &Block,
    resume: Option<&Event>,
) -> Vec<NewAccountActivity> {
    let receiver = event.params[1].as_str().unwrap_or("");
    let is_leg_two = event.name != "TRANSFER_XCHAIN";

    // The event names the far chain only; the near one is the chain the event was emitted on.
    let Some(far_chain) = event.params[3]
        .as_str()
        .and_then(|chain| chain.parse::<i64>().ok())
    else {
        return vec![];
    };

    let (account, direction, source_chain, destination_chain, sender) = if is_leg_two {
        let sender = resume
            .and_then(|resume| recover_xchain_sender(resume, receiver))
            .unwrap_or_default();
        (receiver, "received", far_chain, event.chain_id, sender)
    } else {
        let sender = event.params[0].as_str().unwrap_or("");
        (
            sender,
            "sent",
            event.chain_id,
            far_chain,
            sender.to_string(),
        )
    };

    if account.is_empty() {
        return vec![];
    }

    let mut activity = new_activity(
        event,
        block,
        account.to_string(),
        json!({
            "direction": direction,
            "from": sender,
            "to": receiver,
            "amount": parse_amount(&event.params[2]),
            "xchain": {
                "source_chain": source_chain,
                "destination_chain": destination_chain,
            },
        }),
    );

    // A self-transfer puts both legs in one feed. Leg 2 is still written — completion is derived
    // from both legs sharing a pact id — but typed so it drops out of a TRANSFER-filtered feed,
    // leaving leg 1 as the single entry. An unrecovered sender stays a TRANSFER: two entries is
    // a better failure than a vanished receipt.
    if is_leg_two && sender == receiver {
        activity.activity_type = "XCHAIN_RECEIPT".to_string();
    }

    vec![activity]
}

/// Parse transfer events into account activities.
///
/// A same-chain transfer comes from `TRANSFER`; a cross-chain one comes from its XCHAIN event,
/// which is the only place the real counterparty and the other chain appear.
pub fn parse_transfer_activities(
    events: &[Event],
    blocks: &[Block],
    context: &ActivityContext,
) -> Vec<NewAccountActivity> {
    let blocks_by_hash = blocks
        .iter()
        .map(|block| (block.hash.to_string(), block))
        .collect::<HashMap<String, &Block>>();

    // Keyed by transaction, since X_RESUME only speaks for the leg emitted beside it.
    let resumes_by_tx = events
        .iter()
        .filter(|event| event.name == "X_RESUME")
        .map(|event| ((event.block.as_str(), event.request_key.as_str()), event))
        .collect::<HashMap<(&str, &str), &Event>>();

    events
        .iter()
        .flat_map(|event| {
            let Some(block) = blocks_by_hash.get(&event.block) else {
                return vec![];
            };

            match event.name.as_str() {
                "TRANSFER" => same_chain_activities(event, block, context),
                "TRANSFER_XCHAIN" | "TRANSFER_XCHAIN_RECD" => cross_chain_activity(
                    event,
                    block,
                    resumes_by_tx
                        .get(&(event.block.as_str(), event.request_key.as_str()))
                        .copied(),
                ),
                _ => vec![],
            }
        })
        .collect()
}

/// Fully qualified fungible module name from a supported DEX token reference.
fn token_name(token: &serde_json::Value) -> Option<String> {
    let reference = token.get("refName")?;
    let name = reference.get("name")?.as_str()?;
    let namespace = reference.get("namespace").and_then(|value| value.as_str());

    Some(match namespace {
        Some(namespace) if !namespace.is_empty() => format!("{namespace}.{name}"),
        _ => name.to_string(),
    })
}

fn matching_transfers<'a>(
    events: &[&'a Event],
    token: &str,
    from: Option<&str>,
    to: Option<&str>,
) -> Vec<&'a Event> {
    events
        .iter()
        .copied()
        .filter(|event| event.name == "TRANSFER" && event.module == token)
        .filter_map(|event| {
            let event_from = event.params.get(0)?.as_str()?;
            let event_to = event.params.get(1)?.as_str()?;
            if from.map_or(true, |expected| event_from == expected)
                && to.map_or(true, |expected| event_to == expected)
            {
                Some(event)
            } else {
                None
            }
        })
        .collect()
}

#[derive(Debug)]
struct RecognizedSwap {
    activities: Vec<NewAccountActivity>,
    /// Event indexes are unique inside a transaction and are also what activity identity uses.
    consumed_transfer_indexes: Vec<i64>,
}

fn transfers_for_swap<'a>(
    events: &[&'a Event],
    claimed: &HashSet<i64>,
    token: &str,
    from: Option<&str>,
    to: Option<&str>,
) -> Vec<&'a Event> {
    matching_transfers(events, token, from, to)
        .into_iter()
        .filter(|event| !claimed.contains(&event.idx))
        .collect()
}

/// Split ordered swap events into routes. A hop belongs to the preceding route only when both
/// its pool and input asset continue that route; disconnected swaps are independent swaps.
fn swap_routes(swaps: Vec<&Event>) -> Vec<Vec<&Event>> {
    let mut routes: Vec<Vec<&Event>> = Vec::new();

    for swap in swaps {
        let continues = routes
            .last()
            .and_then(|route| route.last())
            .is_some_and(|previous| {
                previous.params.get(1).and_then(|value| value.as_str())
                    == swap.params.get(0).and_then(|value| value.as_str())
                    && previous.params.get(5).and_then(token_name)
                        == swap.params.get(3).and_then(token_name)
                    && previous.params.get(4).map(parse_amount)
                        == swap.params.get(2).map(parse_amount)
                    && previous.module == swap.module
            });

        if continues {
            routes
                .last_mut()
                .expect("a continuing route exists")
                .push(swap);
        } else {
            routes.push(vec![swap]);
        }
    }

    routes
}

fn recognize_swap_route(
    route: &[&Event],
    transaction_events: &[&Event],
    block: &Block,
    claimed: &HashSet<i64>,
    context: &ActivityContext,
) -> Option<RecognizedSwap> {
    let first = *route.first()?;
    let last = *route.last()?;
    let dex = supported_dex(&first.module)?;
    let token_in = first.params.get(3).and_then(token_name)?;
    let token_out = last.params.get(5).and_then(token_name)?;

    let mut assets = vec![token_in.clone()];
    for swap in route {
        let hop_in = swap.params.get(3).and_then(token_name)?;
        let hop_out = swap.params.get(5).and_then(token_name)?;
        if assets.last() != Some(&hop_in) {
            return None;
        }
        assets.push(hop_out);
    }

    let first_pool = first.params.get(0).and_then(|value| value.as_str())?;
    let last_pool = last.params.get(0).and_then(|value| value.as_str())?;
    let recipient = last.params.get(1).and_then(|value| value.as_str())?;
    let input_transfers = transfers_for_swap(
        transaction_events,
        claimed,
        &token_in,
        None,
        Some(first_pool),
    );
    let output_transfers = transfers_for_swap(
        transaction_events,
        claimed,
        &token_out,
        Some(last_pool),
        Some(recipient),
    );
    if input_transfers.len() != 1 || output_transfers.len() != 1 {
        return None;
    }
    let input = input_transfers[0];
    let output = output_transfers[0];
    if input.params.get(2).map(parse_amount) != first.params.get(2).map(parse_amount)
        || output.params.get(2).map(parse_amount) != last.params.get(4).map(parse_amount)
    {
        return None;
    }
    let source = input.params.get(0).and_then(|value| value.as_str())?;
    if source.is_empty() || recipient.is_empty() {
        return None;
    }

    let mut details = json!({
        "from": source,
        "to": recipient,
        "token_in": token_in,
        "amount_in": parse_amount(&first.params[2]),
        "token_out": token_out,
        "amount_out": parse_amount(&last.params[4]),
        "route": assets,
        "dex": dex,
    });
    let mut consumed = vec![input.idx, output.idx];

    // Linx's wallet fee is a coin transfer from the swap source to the configured fee account,
    // emitted immediately after the route's terminal SWAP event. The exact position gives each
    // fee deterministic ownership when a transaction contains multiple swaps. A missing or
    // non-unique candidate is deliberately left visible.
    let wallet_fees = transaction_events
        .iter()
        .copied()
        .filter(|event| !claimed.contains(&event.idx))
        .filter(|event| event.idx == last.idx + 1)
        .filter(|event| event.name == "TRANSFER" && event.module == "coin")
        .filter(|event| event.params.get(0).and_then(|value| value.as_str()) == Some(source))
        .filter(|event| {
            event.params.get(1).and_then(|value| value.as_str())
                == Some(context.wallet_swap_fee_account.as_str())
        })
        .collect::<Vec<_>>();
    if let [wallet_fee] = wallet_fees.as_slice() {
        let amount = wallet_fee.params.get(2).map(parse_amount)?;
        let gross_amount = add_amounts(details.get("amount_in")?.as_str()?, &amount)?;
        let details = details.as_object_mut()?;
        details.insert("amount_in".to_string(), json!(gross_amount));
        details.insert("swap_fee".to_string(), json!(amount));
        consumed.push(wallet_fee.idx);
    }

    let mut accounts = vec![source];
    if recipient != source {
        accounts.push(recipient);
    }
    let activities = accounts
        .into_iter()
        .map(|account| {
            let mut activity = new_activity(first, block, account.to_string(), details.clone());
            activity.activity_type = "SWAP".to_string();
            activity
        })
        .collect();

    Some(RecognizedSwap {
        activities,
        consumed_transfer_indexes: consumed,
    })
}

/// Parse a supported DEX's one-or-more `SWAP` events into logical routes per transaction.
///
/// The event shape is `[pool, recipient, amount-in, token-in, amount-out, token-out]`.
///
fn recognize_swaps(
    events: &[Event],
    blocks: &[Block],
    context: &ActivityContext,
) -> Vec<RecognizedSwap> {
    let blocks_by_hash = blocks
        .iter()
        .map(|block| (block.hash.as_str(), block))
        .collect::<HashMap<&str, &Block>>();
    let mut transaction_keys = Vec::new();

    for event in events
        .iter()
        .filter(|event| event.name == "SWAP" && supported_dex(&event.module).is_some())
    {
        let key = (event.block.as_str(), event.request_key.as_str());
        if !transaction_keys.contains(&key) {
            transaction_keys.push(key);
        }
    }

    transaction_keys
        .into_iter()
        .flat_map(|(block_hash, request_key)| {
            let Some(block) = blocks_by_hash.get(block_hash) else {
                return vec![];
            };
            let transaction_events = events
                .iter()
                .filter(|event| event.block == block_hash && event.request_key == request_key)
                .collect::<Vec<_>>();
            let mut swaps = transaction_events
                .iter()
                .copied()
                .filter(|event| event.name == "SWAP" && supported_dex(&event.module).is_some())
                .collect::<Vec<_>>();
            swaps.sort_by_key(|event| event.idx);

            let mut claimed = HashSet::new();
            swap_routes(swaps)
                .into_iter()
                .filter_map(|route| {
                    let recognized = recognize_swap_route(
                        &route,
                        &transaction_events,
                        block,
                        &claimed,
                        context,
                    )?;
                    claimed.extend(recognized.consumed_transfer_indexes.iter().copied());
                    Some(recognized)
                })
                .collect()
        })
        .collect()
}

pub fn parse_swap_activities(events: &[Event], blocks: &[Block]) -> Vec<NewAccountActivity> {
    recognize_swaps(events, blocks, &ActivityContext::new(&[], HashMap::new()))
        .into_iter()
        .flat_map(|swap| swap.activities)
        .collect()
}

/// Main function to process all account activities from events
pub fn process_account_activities(
    events: &[Event],
    blocks: &[Block],
    context: &ActivityContext,
) -> Vec<NewAccountActivity> {
    let mut activities = Vec::new();

    let swaps = recognize_swaps(events, blocks, context);
    let consumed_by_transaction = swaps.iter().fold(
        HashMap::<(&str, &str), HashSet<i64>>::new(),
        |mut consumed, swap| {
            if let Some(activity) = swap.activities.first() {
                consumed
                    .entry((&activity.block, &activity.request_key))
                    .or_default()
                    .extend(swap.consumed_transfer_indexes.iter().copied());
            }
            consumed
        },
    );

    activities.extend(
        parse_transfer_activities(events, blocks, context)
            .into_iter()
            .filter(|activity| {
                !consumed_by_transaction
                    .get(&(activity.block.as_str(), activity.request_key.as_str()))
                    .is_some_and(|indexes| indexes.contains(&activity.idx))
            }),
    );
    activities.extend(swaps.into_iter().flat_map(|swap| swap.activities));

    // Future: Add more activity types here
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
    const TEST_WALLET_SWAP_FEE_ACCOUNT: &str = "k:test-wallet-swap-fees";

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
        module_transfer_event(idx, "coin", from, to, json!(amount))
    }

    fn module_transfer_event(
        idx: i64,
        module: &str,
        from: &str,
        to: &str,
        amount: serde_json::Value,
    ) -> Event {
        Event {
            block: BLOCK.to_string(),
            chain_id: 2,
            height: 7001001,
            idx,
            module: module.to_string(),
            module_hash: "hash".to_string(),
            name: "TRANSFER".to_string(),
            params: json!([from, to, amount]),
            param_text: String::new(),
            qual_name: format!("{module}.TRANSFER"),
            request_key: REQUEST_KEY.to_string(),
            pact_id: None,
        }
    }

    fn token_reference(module: &str) -> serde_json::Value {
        let (namespace, name) = module
            .rsplit_once('.')
            .map(|(namespace, name)| (json!(namespace), name))
            .unwrap_or((serde_json::Value::Null, module));
        json!({ "refName": { "namespace": namespace, "name": name } })
    }

    fn kaddex_swap_event(
        idx: i64,
        pool: &str,
        recipient: &str,
        amount_in: serde_json::Value,
        token_in: &str,
        amount_out: serde_json::Value,
        token_out: &str,
    ) -> Event {
        dex_swap_event(
            idx,
            "kaddex.exchange",
            pool,
            recipient,
            amount_in,
            token_in,
            amount_out,
            token_out,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dex_swap_event(
        idx: i64,
        module: &str,
        pool: &str,
        recipient: &str,
        amount_in: serde_json::Value,
        token_in: &str,
        amount_out: serde_json::Value,
        token_out: &str,
    ) -> Event {
        Event {
            block: BLOCK.to_string(),
            chain_id: 2,
            height: 7001001,
            idx,
            module: module.to_string(),
            module_hash: "dex-hash".to_string(),
            name: "SWAP".to_string(),
            params: json!([
                pool,
                recipient,
                amount_in,
                token_reference(token_in),
                amount_out,
                token_reference(token_out),
            ]),
            param_text: String::new(),
            qual_name: format!("{module}.SWAP"),
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
        ActivityContext::with_wallet_swap_fee_account(
            &[transaction(224, 1e-8, sender)],
            HashMap::from([(BLOCK.to_string(), MINER.to_string())]),
            TEST_WALLET_SWAP_FEE_ACCOUNT.to_string(),
        )
    }

    #[test]
    fn amounts_are_rendered_without_scientific_notation() {
        assert_eq!(parse_amount(&json!(2.48314e-7)), "0.000000248314");
        assert_eq!(
            parse_amount(&json!({ "decimal": "2.48314e-7" })),
            "0.000000248314"
        );
    }

    #[test]
    fn the_gas_transfer_does_not_become_an_activity() {
        let events = vec![
            transfer_event(0, SENDER, RECIPIENT, 0.1),
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

    #[test]
    fn a_single_hop_kaddex_route_becomes_one_swap() {
        const POOL: &str = "-qXygkgY9Y_Ld50QdjBng12LIXRqki4EovuF_OJ6DmA";
        const USDC: &str = "n_e595727b657fbbb3b8e362a05a7bb8d12865c1ff.kb-USDC";
        let events = vec![
            transfer_event(0, SENDER, MINER, 1.907e-5),
            module_transfer_event(1, "coin", SENDER, POOL, json!(753.56)),
            module_transfer_event(2, USDC, POOL, SENDER, json!(2.837429)),
            kaddex_swap_event(
                4,
                POOL,
                SENDER,
                json!(753.56),
                "coin",
                json!(2.837429),
                USDC,
            ),
        ];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));
        let swaps = activities
            .iter()
            .filter(|activity| activity.activity_type == "SWAP")
            .collect::<Vec<_>>();

        assert_eq!(swaps.len(), 1);
        let swap = swaps[0];
        assert_eq!(swap.account, SENDER);
        assert_eq!(swap.module_name, "kaddex.exchange");
        assert_eq!(swap.idx, 4);
        assert_eq!(swap.details["from"], json!(SENDER));
        assert_eq!(swap.details["to"], json!(SENDER));
        assert_eq!(swap.details["token_in"], json!("coin"));
        assert_eq!(swap.details["amount_in"], json!("753.56"));
        assert_eq!(swap.details["token_out"], json!(USDC));
        assert_eq!(swap.details["amount_out"], json!("2.837429"));
        assert_eq!(swap.details["route"], json!(["coin", USDC]));
    }

    #[test]
    fn a_multi_hop_kaddex_route_is_collapsed_to_its_outer_assets() {
        const POOL_ONE: &str = "-qXygkgY9Y_Ld50QdjBng12LIXRqki4EovuF_OJ6DmA";
        const POOL_TWO: &str = "Z4qQZwCnb21a_7zv3AMoRTTXFvWnm_IYljV7O4QXBZo";
        const USDC: &str = "n_e595727b657fbbb3b8e362a05a7bb8d12865c1ff.kb-USDC";
        const ETH: &str = "n_e595727b657fbbb3b8e362a05a7bb8d12865c1ff.kb-ETH";
        let events = vec![
            module_transfer_event(0, USDC, SENDER, POOL_ONE, json!(4.931164)),
            module_transfer_event(1, "coin", POOL_ONE, POOL_TWO, json!(1300.972051713535_f64)),
            kaddex_swap_event(
                3,
                POOL_ONE,
                POOL_TWO,
                json!(4.931164),
                USDC,
                json!(1300.972051713535_f64),
                "coin",
            ),
            module_transfer_event(4, ETH, POOL_TWO, SENDER, json!(0.002)),
            kaddex_swap_event(
                6,
                POOL_TWO,
                SENDER,
                json!(1300.972051713535_f64),
                "coin",
                json!({ "decimal": "0.002" }),
                ETH,
            ),
            transfer_event(7, SENDER, MINER, 3.587e-5),
        ];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));
        let swap = activities
            .iter()
            .find(|activity| activity.activity_type == "SWAP")
            .expect("one logical swap");

        assert_eq!(
            activities
                .iter()
                .filter(|activity| activity.activity_type == "SWAP")
                .count(),
            1
        );
        assert_eq!(swap.idx, 3);
        assert_eq!(swap.details["token_in"], json!(USDC));
        assert_eq!(swap.details["amount_in"], json!("4.931164"));
        assert_eq!(swap.details["token_out"], json!(ETH));
        assert_eq!(swap.details["amount_out"], json!("0.002"));
        assert_eq!(swap.details["route"], json!([USDC, "coin", ETH]));
    }

    #[test]
    fn a_kdswap_route_uses_the_shared_swap_schema() {
        const POOL: &str = "dAmjJNY06Tfo3pCrwtWC_3u2FOEwVY5A2jFqImBhxNs";
        const FLUX: &str = "runonflux.flux";
        let events = vec![
            module_transfer_event(0, "coin", SENDER, POOL, json!(94.534708394909_f64)),
            module_transfer_event(1, FLUX, POOL, SENDER, json!(6.66333187)),
            dex_swap_event(
                3,
                "kdlaunch.kdswap-exchange",
                POOL,
                SENDER,
                json!(94.534708394909_f64),
                "coin",
                json!(6.66333187),
                FLUX,
            ),
            // An unrelated transfer in the same call must not be selected as a swap leg.
            module_transfer_event(4, "coin", SENDER, RECIPIENT, json!(0.666407813458_f64)),
            transfer_event(5, GAS_STATION, MINER, 0.001407),
        ];

        let activities = process_account_activities(&events, &[block()], &context(GAS_STATION));
        let swaps = activities
            .iter()
            .filter(|activity| activity.activity_type == "SWAP")
            .collect::<Vec<_>>();

        assert_eq!(swaps.len(), 1);
        let swap = swaps[0];
        assert_eq!(swap.account, SENDER);
        assert_eq!(swap.module_name, "kdlaunch.kdswap-exchange");
        assert_eq!(swap.details["dex"], json!("kdswap"));
        assert_eq!(swap.details["token_in"], json!("coin"));
        assert_eq!(swap.details["amount_in"], json!("94.534708394909"));
        assert_eq!(swap.details["token_out"], json!(FLUX));
        assert_eq!(swap.details["amount_out"], json!("6.66333187"));
        assert_eq!(swap.details["route"], json!(["coin", FLUX]));
    }

    #[test]
    fn a_swap_replaces_its_transfers_and_carries_both_kinds_of_fee() {
        const POOL: &str = "pool";
        const TOKEN: &str = "free.token";
        const WALLET: &str = TEST_WALLET_SWAP_FEE_ACCOUNT;
        let events = vec![
            module_transfer_event(0, "coin", SENDER, POOL, json!({ "decimal": "0.0993" })),
            module_transfer_event(
                1,
                TOKEN,
                POOL,
                SENDER,
                json!({ "decimal": "0.000000248314" }),
            ),
            dex_swap_event(
                3,
                "kdlaunch.kdswap-exchange",
                POOL,
                SENDER,
                json!({ "decimal": "0.0993" }),
                "coin",
                json!({ "decimal": "0.000000248314" }),
                TOKEN,
            ),
            module_transfer_event(4, "coin", SENDER, WALLET, json!({ "decimal": "0.0007" })),
        ];
        let context = context(GAS_STATION);

        let activities = process_account_activities(&events, &[block()], &context);

        assert_eq!(activities.len(), 1);
        let swap = &activities[0];
        assert_eq!(swap.activity_type, "SWAP");
        assert_eq!(swap.idx, 3, "the SWAP event remains the activity identity");
        assert_eq!(swap.details["amount_in"], json!("0.1"));
        assert_eq!(swap.details["swap_fee"], json!("0.0007"));
        assert_eq!(swap.details["network_fee"], json!("0.00000224"));
        assert_eq!(swap.details["fee_payer"], json!(GAS_STATION));
        assert_ne!(swap.details["swap_fee"], swap.details["network_fee"]);

        let raw = parse_transfer_activities(&events, &[block()], &context);
        assert_eq!(raw.len(), 6, "raw transfer parsing remains complete");
    }

    #[test]
    fn a_fee_like_transfer_to_another_account_is_not_merged() {
        const POOL: &str = "pool";
        const TOKEN: &str = "free.token";
        let events = vec![
            module_transfer_event(0, "coin", SENDER, POOL, json!(0.0993)),
            module_transfer_event(1, TOKEN, POOL, SENDER, json!(2)),
            dex_swap_event(
                3,
                "kdlaunch.kdswap-exchange",
                POOL,
                SENDER,
                json!(0.0993),
                "coin",
                json!(2),
                TOKEN,
            ),
            module_transfer_event(4, "coin", SENDER, RECIPIENT, json!(0.0007)),
        ];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));
        let swap = activities
            .iter()
            .find(|activity| activity.activity_type == "SWAP")
            .unwrap();

        assert_eq!(swap.details["amount_in"], json!("0.0993"));
        assert!(swap.details.get("swap_fee").is_none());
        assert_eq!(
            activities
                .iter()
                .filter(|activity| activity.idx == 4)
                .count(),
            2
        );
    }

    #[test]
    fn unrelated_and_ambiguous_transfers_remain_visible() {
        const POOL: &str = "pool";
        const TOKEN: &str = "free.token";
        let events = vec![
            module_transfer_event(0, "coin", SENDER, POOL, json!(1)),
            // A second exact input makes association ambiguous, so no transfer may be hidden.
            module_transfer_event(1, "coin", SENDER, POOL, json!(1)),
            module_transfer_event(2, TOKEN, POOL, SENDER, json!(2)),
            dex_swap_event(
                4,
                "kdlaunch.kdswap-exchange",
                POOL,
                SENDER,
                json!(1),
                "coin",
                json!(2),
                TOKEN,
            ),
            module_transfer_event(5, "coin", SENDER, RECIPIENT, json!(9)),
        ];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));

        assert!(activities
            .iter()
            .all(|activity| activity.activity_type == "TRANSFER"));
        assert_eq!(activities.len(), 8);
        assert_eq!(
            activities
                .iter()
                .filter(|activity| activity.idx == 5)
                .count(),
            2
        );
    }

    #[test]
    fn multiple_swaps_claim_disjoint_transfers() {
        const POOL_ONE: &str = "pool-one";
        const POOL_TWO: &str = "pool-two";
        const TOKEN_ONE: &str = "free.one";
        const TOKEN_TWO: &str = "free.two";
        let events = vec![
            module_transfer_event(0, "coin", SENDER, POOL_ONE, json!(1)),
            module_transfer_event(1, TOKEN_ONE, POOL_ONE, SENDER, json!(2)),
            dex_swap_event(
                3,
                "kdlaunch.kdswap-exchange",
                POOL_ONE,
                SENDER,
                json!(1),
                "coin",
                json!(2),
                TOKEN_ONE,
            ),
            module_transfer_event(4, TOKEN_ONE, SENDER, POOL_TWO, json!(0.5)),
            module_transfer_event(5, TOKEN_TWO, POOL_TWO, SENDER, json!(3)),
            dex_swap_event(
                7,
                "kdlaunch.kdswap-exchange",
                POOL_TWO,
                SENDER,
                json!(0.5),
                TOKEN_ONE,
                json!(3),
                TOKEN_TWO,
            ),
        ];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));
        let swaps = activities
            .iter()
            .filter(|activity| activity.activity_type == "SWAP")
            .collect::<Vec<_>>();

        assert_eq!(swaps.len(), 2);
        assert_eq!(swaps[0].idx, 3);
        assert_eq!(swaps[1].idx, 7);
        assert_eq!(
            swaps
                .iter()
                .filter(|swap| swap.details.get("network_fee").is_some())
                .count(),
            1
        );
        assert!(activities
            .iter()
            .all(|activity| activity.activity_type != "TRANSFER"));
    }

    #[test]
    fn a_swap_owns_the_network_fee_even_when_an_unrelated_transfer_has_a_lower_index() {
        const POOL: &str = "pool";
        const TOKEN: &str = "free.token";
        let events = vec![
            module_transfer_event(0, "coin", SENDER, RECIPIENT, json!(9)),
            module_transfer_event(1, "coin", SENDER, POOL, json!(1)),
            module_transfer_event(2, TOKEN, POOL, SENDER, json!(2)),
            dex_swap_event(
                4,
                "kdlaunch.kdswap-exchange",
                POOL,
                SENDER,
                json!(1),
                "coin",
                json!(2),
                TOKEN,
            ),
        ];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));
        let swap = activities
            .iter()
            .find(|activity| activity.activity_type == "SWAP")
            .unwrap();

        assert!(swap.details.get("network_fee").is_some());
        assert!(activities
            .iter()
            .filter(|activity| activity.account == SENDER)
            .filter(|activity| {
                activity.details.get("fee").is_some()
                    || activity.details.get("network_fee").is_some()
            })
            .all(|activity| activity.activity_type == "SWAP"));
        assert_eq!(
            activities
                .iter()
                .filter(|activity| activity.idx == 0)
                .count(),
            2
        );
    }

    #[test]
    fn a_disconnected_kaddex_route_is_not_indexed() {
        const POOL_ONE: &str = "pool-one";
        const POOL_TWO: &str = "pool-two";
        let events = vec![
            module_transfer_event(0, "coin", SENDER, POOL_ONE, json!(1)),
            kaddex_swap_event(1, POOL_ONE, POOL_TWO, json!(1), "coin", json!(2), "token-a"),
            module_transfer_event(2, "token-b", POOL_TWO, SENDER, json!(3)),
            kaddex_swap_event(
                3,
                POOL_TWO,
                SENDER,
                json!(2),
                "token-b",
                json!(3),
                "token-c",
            ),
        ];

        let activities = process_account_activities(&events, &[block()], &context(SENDER));

        assert!(activities
            .iter()
            .all(|activity| activity.activity_type != "SWAP"));
    }

    // Mainnet pact EZyiBHNB_OWlYI-3bO4ROlhmKv5QCvWVCzPjLUn_axg: 1 KDA from chain 1 to chain 7,
    // sender and receiver the same account.
    const PACT_ID: &str = "EZyiBHNB_OWlYI-3bO4ROlhmKv5QCvWVCzPjLUn_axg";
    const XCHAIN_ACCOUNT: &str = RECIPIENT;
    /// Leg 2 is paid for by a gas station, not by the sender.
    const XCHAIN_GAS: &str = "kadena-xchain-gas";
    const LEG_ONE_BLOCK: &str = "leg-one-block-hash";
    const LEG_TWO_BLOCK: &str = "leg-two-block-hash";
    const LEG_TWO_REQUEST_KEY: &str = "s8Hs0Rn1SNQXQ9tZgLhqoLBAAf2WvHjnpQmL0hpP3Rw";

    fn leg_block(chain_id: i64, hash: &str) -> Block {
        Block {
            chain_id,
            creation_time: timestamp(),
            hash: hash.to_string(),
            height: 7001001,
            parent: "parent".to_string(),
        }
    }

    fn leg_event(
        chain_id: i64,
        block_hash: &str,
        request_key: &str,
        idx: i64,
        name: &str,
        params: serde_json::Value,
    ) -> Event {
        Event {
            block: block_hash.to_string(),
            chain_id,
            height: 7001001,
            idx,
            module: "coin".to_string(),
            module_hash: "hash".to_string(),
            name: name.to_string(),
            params,
            param_text: String::new(),
            qual_name: format!("coin.{name}"),
            request_key: request_key.to_string(),
            pact_id: Some(PACT_ID.to_string()),
        }
    }

    /// `pact.X_RESUME`, emitted on leg 2 with the defpact's original arguments.
    fn resume_event(defpact: &str, args: serde_json::Value) -> Event {
        Event {
            module: "pact".to_string(),
            qual_name: "pact.X_RESUME".to_string(),
            ..leg_event(
                7,
                LEG_TWO_BLOCK,
                LEG_TWO_REQUEST_KEY,
                3,
                "X_RESUME",
                json!(["1", defpact, args]),
            )
        }
    }

    fn xchain_resume(sender: &str, receiver: &str) -> Event {
        resume_event(
            "coin.transfer-crosschain",
            json!([sender, receiver, { "pred": "keys-all", "keys": ["3787e2"] }, "7", 1]),
        )
    }

    fn leg_context(block_hash: &str, request_key: &str, gas: i64, payer: &str) -> ActivityContext {
        let tx = Transaction {
            block: block_hash.to_string(),
            request_key: request_key.to_string(),
            ..transaction(gas, 1e-8, payer)
        };
        ActivityContext::with_wallet_swap_fee_account(
            &[tx],
            HashMap::from([(block_hash.to_string(), MINER.to_string())]),
            TEST_WALLET_SWAP_FEE_ACCOUNT.to_string(),
        )
    }

    /// Chain 1, step 0: the burn.
    fn leg_one() -> Vec<Event> {
        vec![
            leg_event(
                1,
                LEG_ONE_BLOCK,
                PACT_ID,
                0,
                "TRANSFER_XCHAIN",
                json!([XCHAIN_ACCOUNT, XCHAIN_ACCOUNT, 1, "7"]),
            ),
            leg_event(
                1,
                LEG_ONE_BLOCK,
                PACT_ID,
                1,
                "TRANSFER",
                json!([XCHAIN_ACCOUNT, "", 1]),
            ),
            leg_event(
                1,
                LEG_ONE_BLOCK,
                PACT_ID,
                2,
                "TRANSFER",
                json!([GAS_STATION, MINER, 2.89e-06]),
            ),
        ]
    }

    /// Chain 7, step 1: the mint, minutes later. The chain names no sender here — only the
    /// `X_RESUME`, when one is present, does.
    fn leg_two_receiving(receiver: &str, resume: Option<Event>) -> Vec<Event> {
        let mut events = vec![
            leg_event(
                7,
                LEG_TWO_BLOCK,
                LEG_TWO_REQUEST_KEY,
                0,
                "TRANSFER",
                json!(["", receiver, 1]),
            ),
            leg_event(
                7,
                LEG_TWO_BLOCK,
                LEG_TWO_REQUEST_KEY,
                1,
                "TRANSFER_XCHAIN_RECD",
                json!(["", receiver, 1, "1"]),
            ),
            leg_event(
                7,
                LEG_TWO_BLOCK,
                LEG_TWO_REQUEST_KEY,
                2,
                "TRANSFER",
                json!([XCHAIN_GAS, MINER, 3.24e-06]),
            ),
        ];
        events.extend(resume);
        events
    }

    /// The mainnet sample: sender and receiver are the same account.
    fn leg_two() -> Vec<Event> {
        leg_two_receiving(
            XCHAIN_ACCOUNT,
            Some(xchain_resume(XCHAIN_ACCOUNT, XCHAIN_ACCOUNT)),
        )
    }

    fn leg_one_activities() -> Vec<NewAccountActivity> {
        process_account_activities(
            &leg_one(),
            &[leg_block(1, LEG_ONE_BLOCK)],
            &leg_context(LEG_ONE_BLOCK, PACT_ID, 289, GAS_STATION),
        )
    }

    fn activities_for(leg_two_events: &[Event]) -> Vec<NewAccountActivity> {
        process_account_activities(
            leg_two_events,
            &[leg_block(7, LEG_TWO_BLOCK)],
            &leg_context(LEG_TWO_BLOCK, LEG_TWO_REQUEST_KEY, 324, XCHAIN_GAS),
        )
    }

    fn leg_two_activities() -> Vec<NewAccountActivity> {
        activities_for(&leg_two())
    }

    #[test]
    fn leg_one_is_the_sender_paying_out_to_another_chain() {
        let activities = leg_one_activities();

        assert_eq!(activities.len(), 1, "only the sender has moved anything");
        let sent = &activities[0];
        assert_eq!(sent.account, XCHAIN_ACCOUNT);
        assert_eq!(sent.activity_type, "TRANSFER");
        assert_eq!(sent.chain_id, 1);
        assert_eq!(sent.pact_id.as_deref(), Some(PACT_ID));
        assert_eq!(sent.details["direction"], json!("sent"));
        assert_eq!(sent.details["from"], json!(XCHAIN_ACCOUNT));
        assert_eq!(
            sent.details["to"],
            json!(XCHAIN_ACCOUNT),
            "the real receiver, not the burn's empty side"
        );
        assert_eq!(sent.details["amount"], json!("1"));
        assert_eq!(sent.details["xchain"]["source_chain"], json!(1));
        assert_eq!(sent.details["xchain"]["destination_chain"], json!(7));
    }

    #[test]
    fn leg_two_of_a_self_transfer_is_a_receipt_rather_than_a_second_entry() {
        let activities = leg_two_activities();

        assert_eq!(activities.len(), 1, "only the receiver is credited");
        let received = &activities[0];
        assert_eq!(received.account, XCHAIN_ACCOUNT);
        assert_eq!(received.chain_id, 7);
        assert_eq!(received.pact_id.as_deref(), Some(PACT_ID));
        assert_eq!(
            received.activity_type, "XCHAIN_RECEIPT",
            "sender and receiver share a feed, so leg 1 is the only TRANSFER in it"
        );
        assert_eq!(received.details["direction"], json!("received"));
        assert_eq!(received.details["to"], json!(XCHAIN_ACCOUNT));
        assert_eq!(received.details["amount"], json!("1"));
        assert_eq!(received.details["xchain"]["source_chain"], json!(1));
        assert_eq!(received.details["xchain"]["destination_chain"], json!(7));
        // TRANSFER_XCHAIN_RECD carries an empty sender; X_RESUME is where it survives.
        assert_eq!(received.details["from"], json!(XCHAIN_ACCOUNT));
    }

    #[test]
    fn leg_two_of_a_cross_account_transfer_is_a_transfer_with_the_recovered_sender() {
        let events = leg_two_receiving(RECIPIENT, Some(xchain_resume(SENDER, RECIPIENT)));

        let activities = activities_for(&events);

        assert_eq!(
            activities.len(),
            1,
            "the X_RESUME itself is not an activity"
        );
        let received = &activities[0];
        assert_eq!(received.account, RECIPIENT);
        assert_eq!(
            received.activity_type, "TRANSFER",
            "the receiver's feed has no other entry for this transfer"
        );
        assert_eq!(received.details["direction"], json!("received"));
        assert_eq!(received.details["from"], json!(SENDER));
        assert_eq!(received.details["to"], json!(RECIPIENT));
        assert_eq!(received.details["xchain"]["source_chain"], json!(1));
        assert_eq!(received.details["xchain"]["destination_chain"], json!(7));
    }

    #[test]
    fn a_resume_disagreeing_on_the_receiver_is_not_trusted_for_the_sender() {
        // Position 1 does not hold the receiver the chain reported, so position 0 is not the
        // sender either — the argument order is not what we assumed.
        let events = leg_two_receiving(RECIPIENT, Some(xchain_resume(SENDER, GAS_STATION)));

        let activities = activities_for(&events);

        assert_eq!(activities.len(), 1);
        assert_eq!(
            activities[0].activity_type, "TRANSFER",
            "an ordinary transfer beats a receipt attributed to the wrong account"
        );
        assert_eq!(activities[0].details["from"], json!(""));
    }

    #[test]
    fn a_resume_for_another_defpact_says_nothing_about_the_sender() {
        let resume = resume_event(
            "free.some-bridge.transfer",
            json!([SENDER, RECIPIENT, "7", 1]),
        );
        let events = leg_two_receiving(RECIPIENT, Some(resume));

        let activities = activities_for(&events);

        assert_eq!(activities.len(), 1);
        assert_eq!(activities[0].activity_type, "TRANSFER");
        assert_eq!(activities[0].details["from"], json!(""));
    }

    #[test]
    fn leg_two_without_a_resume_stays_an_ordinary_transfer() {
        let activities = activities_for(&leg_two_receiving(XCHAIN_ACCOUNT, None));

        assert_eq!(activities.len(), 1);
        assert_eq!(
            activities[0].activity_type, "TRANSFER",
            "with no sender to compare, a self-transfer cannot be recognised as one"
        );
        assert_eq!(activities[0].details["from"], json!(""));
    }

    #[test]
    fn both_legs_agree_on_which_chains_the_transfer_spans() {
        let sent = leg_one_activities();
        let cross_account = activities_for(&leg_two_receiving(
            RECIPIENT,
            Some(xchain_resume(SENDER, RECIPIENT)),
        ));

        for received in [leg_two_activities(), cross_account] {
            assert_eq!(sent[0].details["xchain"], received[0].details["xchain"]);
            assert_eq!(sent[0].pact_id, received[0].pact_id);
        }
    }

    #[test]
    fn a_transfer_with_an_empty_side_never_produces_a_row() {
        let burn_and_mint = vec![
            leg_event(
                1,
                LEG_ONE_BLOCK,
                PACT_ID,
                1,
                "TRANSFER",
                json!([XCHAIN_ACCOUNT, "", 1]),
            ),
            leg_event(
                7,
                LEG_TWO_BLOCK,
                LEG_TWO_REQUEST_KEY,
                0,
                "TRANSFER",
                json!(["", XCHAIN_ACCOUNT, 1]),
            ),
        ];
        let blocks = [leg_block(1, LEG_ONE_BLOCK), leg_block(7, LEG_TWO_BLOCK)];

        let activities = process_account_activities(
            &burn_and_mint,
            &blocks,
            &ActivityContext::with_wallet_swap_fee_account(
                &[],
                HashMap::new(),
                TEST_WALLET_SWAP_FEE_ACCOUNT.to_string(),
            ),
        );

        assert!(
            activities.is_empty(),
            "the burn and the mint are artifacts, not activities"
        );
    }

    #[test]
    fn each_leg_carries_its_own_fee_and_payer() {
        let sent = leg_one_activities();
        let received = leg_two_activities();

        assert_eq!(sent[0].details["fee"], json!("0.00000289"));
        assert_eq!(sent[0].details["fee_payer"], json!(GAS_STATION));

        assert_eq!(received[0].details["fee"], json!("0.00000324"));
        assert_eq!(
            received[0].details["fee_payer"],
            json!(XCHAIN_GAS),
            "leg 2 is sponsored, so the payer is not the account"
        );
        assert_ne!(received[0].details["fee_payer"], json!(XCHAIN_ACCOUNT));
    }
}
