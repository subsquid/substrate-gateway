use crate::entities::{Block, BlockHeader, Extrinsic, Call, Event};
use sqlx::{postgres::PgRow, Error, Pool, Postgres, Row};


pub async fn get_blocks(pool: &Pool<Postgres>, limit: i32) -> Result<Vec<Block>, Error> {
    let query = "SELECT id, height, hash, parent_hash, timestamp FROM block LIMIT $1";
    let blocks = sqlx::query(query)
        .bind(limit)
        .map(|row: PgRow| Block {
            header: BlockHeader {
                id: row.get_unchecked("id"),
                height: row.get_unchecked("height"),
                hash: row.get_unchecked("hash"),
                parent_hash: row.get_unchecked("parent_hash"),
                timestamp: row.get_unchecked("timestamp"),
            },
        })
        .fetch_all(pool)
        .await?;
    Ok(blocks)
}


pub async fn get_extrinsics(pool: &Pool<Postgres>, blocks: &[String]) -> Result<Vec<Extrinsic>, Error> {
    let query = "SELECT id, block_id, index_in_block, name, signature, success, hash FROM extrinsic WHERE block_id = ANY($1)";
    let extrinsics = sqlx::query_as::<_, Extrinsic>(query)
        .bind(blocks)
        .fetch_all(pool)
        .await?;
    Ok(extrinsics)
}


pub async fn get_calls(pool: &Pool<Postgres>, blocks: &[String]) -> Result<Vec<Call>, Error> {
    let query = "SELECT
            call.id,
            call.index,
            call.extrinsic_id,
            call.parent_id,
            call.success,
            call.name,
            call.args,
            extrinsic.block_id
        FROM call INNER JOIN extrinsic ON call.extrinsic_id = extrinsic.id
        WHERE extrinsic.block_id = ANY($1)";
    let calls = sqlx::query_as::<_, Call>(query)
        .bind(blocks)
        .fetch_all(pool)
        .await?;
    Ok(calls)
}


pub async fn get_events(pool: &Pool<Postgres>, blocks: &[String]) -> Result<Vec<Event>, Error> {
    let query = "SELECT id, block_id, index_in_block, phase, extrinsic_id, call_id, name, args FROM event WHERE block_id = ANY($1)";
    let events = sqlx::query_as::<_, Event>(query)
        .bind(blocks)
        .fetch_all(pool)
        .await?;
    Ok(events)
}
