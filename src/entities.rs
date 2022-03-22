use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use sqlx::FromRow;


#[derive(SimpleObject, Debug)]
pub struct BlockHeader {
    pub id: String,
    pub height: i32,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: DateTime<Utc>,
    pub spec_version: i32,
}


#[derive(FromRow, Clone, Debug, Serialize, Deserialize)]
pub struct Extrinsic {
    pub id: String,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_in_block: Option<i32>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<serde_json::Value>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success: Option<bool>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing)]
    pub _block_id: String,
}


#[derive(FromRow, Clone, Debug, Serialize, Deserialize)]
pub struct Call {
    pub id: String,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<i32>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extrinsic_id: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success: Option<bool>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<serde_json::Value>,
    #[serde(skip_serializing)]
    pub _block_id: String,
}


#[derive(FromRow, Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_in_block: Option<i32>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extrinsic_id: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[sqlx(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<serde_json::Value>,
    #[serde(skip_serializing)]
    pub _block_id: String,
}


#[derive(Debug)]
pub struct Block {
    pub header: BlockHeader,
}


#[derive(FromRow, SimpleObject, Debug)]
pub struct Metadata {
    spec_version: i32,
    block_height: i32,
    block_hash: String,
    hex: String,
}


#[derive(FromRow, SimpleObject, Debug)]
pub struct Status {
    pub head: i32,
}
