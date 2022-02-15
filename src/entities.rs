use chrono::{DateTime, Utc};


#[derive(sqlx::FromRow, async_graphql::SimpleObject)]
pub struct Block {
    id: String,
    height: i32,
    hash: String,
    parent_hash: String,
    timestamp: DateTime<Utc>,
}
