use chrono::{DateTime, Utc};
use sqlx::FromRow;
use rust_decimal::Decimal;

#[derive(FromRow, Debug)]
pub struct BlockHeader {
    pub id: String,
    pub height: i64,
    pub hash: String,
    pub parent_hash: String,
    pub state_root: String,
    pub extrinsics_root: String,
    pub timestamp: DateTime<Utc>,
    pub spec_id: String,
    pub validator: Option<String>,
}

#[derive(sqlx::FromRow, Debug)]
pub struct Event {
    pub id: String,
    pub block_id: String,
    pub index_in_block: i64,
    pub phase: String,
    pub extrinsic_id: Option<String>,
    pub call_id: Option<String>,
    pub name: String,
    pub args: Option<serde_json::Value>,
    pub pos: i64,
    pub contract: Option<String>,
}

#[derive(sqlx::FromRow, Debug)]
pub struct Call {
    pub id: String,
    pub parent_id: Option<String>,
    pub block_id: String,
    pub extrinsic_id: String,
    pub name: String,
    pub args: Option<serde_json::Value>,
    pub success: bool,
    pub error: Option<serde_json::Value>,
    pub origin: Option<serde_json::Value>,
    pub pos: i64,
}

#[derive(sqlx::FromRow)]
pub struct Extrinsic {
    pub id: String,
    pub block_id: String,
    pub index_in_block: i64,
    pub version: i64,
    pub signature: Option<serde_json::Value>,
    pub call_id: String,
    pub fee: Option<Decimal>,
    pub tip: Option<Decimal>,
    pub success: bool,
    pub error: Option<serde_json::Value>,
    pub pos: i64,
    pub hash: String
}

#[derive(sqlx::FromRow, Debug)]
pub struct EvmLog {
    pub block_id: String,
    pub selection_index: i16,
    pub data: serde_json::Value,
}

#[derive(sqlx::FromRow, Debug)]
pub struct ContractsEvent {
    pub block_id: String,
    pub selection_index: i16,
    pub data: serde_json::Value,
}

#[derive(Debug)]
pub struct Batch {
    pub header: BlockHeader,
    pub extrinsics: Vec<serde_json::Value>,
    pub calls: Vec<serde_json::Value>,
    pub events: Vec<serde_json::Value>,
}

#[derive(FromRow, Debug)]
pub struct Metadata {
    pub id: String,
    pub spec_name: String,
    pub spec_version: i64,
    pub block_height: i64,
    pub block_hash: String,
    pub hex: String,
}

#[derive(FromRow, Debug)]
pub struct Status {
    pub head: i64,
}
