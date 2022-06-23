use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};
use sqlx::FromRow;


#[derive(FromRow, SimpleObject, Debug)]
pub struct BlockHeader {
    pub id: String,
    pub height: i64,
    pub hash: String,
    pub parent_hash: String,
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
    pub block_id: String,
    pub name: String,
    pub data: serde_json::Value,
}


#[derive(sqlx::FromRow, Debug)]
pub struct FullCall {
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


#[derive(sqlx::FromRow, Debug)]
pub struct Extrinsic {
    pub block_id: String,
    pub data: serde_json::Value,
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


#[derive(SimpleObject, Debug)]
pub struct Batch {
    pub header: BlockHeader,
    pub extrinsics: Vec<serde_json::Value>,
    pub calls: Vec<serde_json::Value>,
    pub events: Vec<serde_json::Value>,
}


#[derive(FromRow, SimpleObject, Debug)]
pub struct Metadata {
    pub id: String,
    pub spec_name: String,
    pub spec_version: i64,
    pub block_height: i64,
    pub block_hash: String,
    pub hex: String,
}


#[derive(FromRow, SimpleObject, Debug)]
pub struct Status {
    pub head: i64,
}
