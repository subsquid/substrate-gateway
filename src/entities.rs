use chrono::{DateTime, Utc};


#[derive(sqlx::FromRow, async_graphql::SimpleObject)]
pub struct Block {
    id: String,
    height: i32,
    hash: String,
    parent_hash: String,
    timestamp: DateTime<Utc>,
}


#[derive(sqlx::FromRow, async_graphql::SimpleObject)]
pub struct Extrinsic {
    id: String,
    block_id: String,
    index_in_block: i32,
    name: String,
    signature: Option<serde_json::Value>,
    success: bool,
    hash: Vec<u8>,
}


#[derive(sqlx::FromRow, async_graphql::SimpleObject)]
pub struct Call {
    id: String,
    index: i32,
    extrinsic_id: String,
    parent_id: Option<String>,
    success: bool,
    name: String,
    args: Option<serde_json::Value>,
}


#[derive(sqlx::FromRow, async_graphql::SimpleObject)]
pub struct Event {
    id: String,
    block_id: String,
    index_in_block: i32,
    phase: String,
    extrinsic_id: Option<String>,
    call_id: Option<String>,
    name: String,
    args: Option<serde_json::Value>
}
