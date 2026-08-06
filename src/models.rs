use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::Serialize;

#[derive(Queryable, Selectable, Insertable, Debug, Clone, Serialize)]
#[diesel(table_name = crate::schema::blocks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Block {
    pub chain_id: i64,
    pub creation_time: NaiveDateTime,
    pub hash: String,
    pub height: i64,
    pub parent: String,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone, AsChangeset)]
#[diesel(table_name = crate::schema::events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Event {
    pub block: String,
    pub chain_id: i64,
    pub height: i64,
    pub idx: i64,
    pub module: String,
    pub module_hash: String,
    pub name: String,
    pub params: serde_json::Value,
    pub param_text: String,
    pub qual_name: String,
    pub request_key: String,
    pub pact_id: Option<String>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::transactions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
#[derive(Serialize)]
pub struct Transaction {
    pub bad_result: Option<serde_json::Value>,
    pub block: String,
    pub chain_id: i64,
    pub continuation: Option<serde_json::Value>,
    pub creation_time: NaiveDateTime,
    pub gas: i64,
    pub gas_limit: i64,
    pub gas_price: f64,
    pub good_result: Option<serde_json::Value>,
    pub height: i64,
    pub logs: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub nonce: String,
    pub pact_id: Option<String>,
    pub request_key: String,
    pub rollback: Option<bool>,
    pub sender: String,
    pub step: Option<i64>,
    pub ttl: i64,
    pub tx_id: Option<i64>,
}

#[derive(Queryable, Selectable, Insertable, Associations, Debug, Clone, PartialEq, Eq)]
#[diesel(belongs_to(Block, foreign_key = block))]
#[diesel(table_name = crate::schema::transfers)]
#[diesel(check_for_backend(diesel::pg::Pg))]
#[derive(Serialize)]
pub struct Transfer {
    pub amount: BigDecimal,
    pub block: String,
    pub chain_id: i64,
    pub creation_time: NaiveDateTime,
    pub from_account: String,
    pub height: i64,
    pub idx: i64,
    pub module_hash: String,
    pub module_name: String,
    pub pact_id: Option<String>,
    pub request_key: String,
    pub to_account: String,
}

/// Serialises a stored timestamp as unix **microseconds**.
fn serialize_as_unix_micros<S>(value: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_i64(value.and_utc().timestamp_micros())
}

#[derive(Queryable, Selectable, Associations, Debug, Clone)]
#[diesel(belongs_to(Block, foreign_key = block))]
#[diesel(table_name = crate::schema::account_activities)]
#[diesel(check_for_backend(diesel::pg::Pg))]
#[derive(Serialize)]
pub struct AccountActivity {
    pub id: i64,
    pub account: String,
    pub activity_type: String,
    pub module_name: String,
    pub chain_id: i64,
    pub height: i64,
    pub block: String,
    pub request_key: String,
    #[serde(serialize_with = "serialize_as_unix_micros")]
    pub creation_time: NaiveDateTime,
    pub details: serde_json::Value,
    /// Set only for both steps of a cross-chain transfer
    pub pact_id: Option<String>,
    /// Position of the event within its transaction.
    pub idx: Option<i64>,
}

#[derive(Insertable, Debug, Clone)]
#[diesel(table_name = crate::schema::account_activities)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewAccountActivity {
    pub account: String,
    pub activity_type: String,
    pub module_name: String,
    pub chain_id: i64,
    pub height: i64,
    pub block: String,
    pub request_key: String,
    pub creation_time: NaiveDateTime,
    pub details: serde_json::Value,
    pub pact_id: Option<String>,
    pub idx: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn activity(creation_time: NaiveDateTime) -> AccountActivity {
        AccountActivity {
            id: 1,
            account: "k:abc".to_string(),
            activity_type: "TRANSFER".to_string(),
            module_name: "coin".to_string(),
            chain_id: 2,
            height: 7001001,
            block: "block".to_string(),
            request_key: "rk".to_string(),
            creation_time,
            details: serde_json::json!({"amount": "0.1"}),
            pact_id: None,
            idx: Some(0),
        }
    }

    fn parse(raw: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f").unwrap()
    }

    #[test]
    fn creation_time_is_emitted_as_a_number_not_a_string() {
        let json = serde_json::to_value(activity(parse("2026-06-27T12:45:38.730935"))).unwrap();

        assert_eq!(
            json["creation_time"],
            serde_json::json!(1782564338730935_i64)
        );
        assert!(
            json["creation_time"].is_number(),
            "client's shouldn't have to parse the timestamp"
        );
    }
}
