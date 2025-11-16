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
