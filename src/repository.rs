use crate::entities::{Block, BlockHeader, Extrinsic, Call, Event};
use sqlx::{postgres::PgRow, Error, Pool, Postgres, Row};
use async_graphql::InputObject;


#[derive(InputObject)]
pub struct ExtrinsicFields {
    #[graphql(name="_all")]
    _all: Option<bool>,
    id: Option<bool>,
    block_id: Option<bool>,
    index_in_block: Option<bool>,
    name: Option<bool>,
    signature: Option<bool>,
    success: Option<bool>,
    hash: Option<bool>,
}


#[derive(InputObject)]
pub struct CallFields {
    #[graphql(name="_all")]
    _all: Option<bool>,
    id: Option<bool>,
    extrinsic: Option<ExtrinsicFields>,
    parent: Option<bool>,
    success: Option<bool>,
    name: Option<bool>,
    args: Option<bool>,
}


#[derive(InputObject)]
pub struct EventFields {
    #[graphql(name="_all")]
    _all: Option<bool>,
    id: Option<bool>,
    block_id: Option<bool>,
    index_in_block: Option<bool>,
    phase: Option<bool>,
    extrinsic: Option<ExtrinsicFields>,
    call_id: Option<bool>,
    name: Option<bool>,
    args: Option<bool>,
}


#[derive(InputObject)]
pub struct EventSelection {
    name: String,
    fields: EventFields,
}


#[derive(InputObject)]
pub struct CallSelection {
    name: String,
    fields: CallFields,
}


pub async fn get_blocks(
    pool: &Pool<Postgres>,
    limit: i32,
    from_block: i32,
    to_block: Option<i32>,
    events: Option<Vec<EventSelection>>,
    calls: Option<Vec<CallSelection>>,
    include_all_blocks: Option<bool>
) -> Result<Vec<Block>, Error> {
    let mut events_name: Option<Vec<String>> = None;
    let mut calls_name: Option<Vec<String>> = None;
    if let Some(events) = events {
        events_name = Some(events.iter().map(|selection| selection.name.clone()).collect());
    }
    if let Some(calls) = calls {
        calls_name = Some(calls.iter().map(|selection| selection.name.clone()).collect());
    }
    let query = "SELECT
            id,
            height,
            hash,
            parent_hash,
            timestamp
        FROM block
        WHERE height >= $1
            AND ($2 IS null OR height < $2)
            AND ($3 IS true OR (
                EXISTS (SELECT 1 FROM event WHERE event.block_id = block.id AND event.name = ANY($4))
                OR EXISTS (
                    SELECT 1
                    FROM call INNER JOIN extrinsic ON call.extrinsic_id = extrinsic.id
                    WHERE extrinsic.block_id = block.id AND call.name = ANY($5)
                )
            ))
        ORDER BY height
        LIMIT $6";
    let blocks = sqlx::query(query)
        .bind(from_block)
        .bind(to_block)
        .bind(include_all_blocks)
        .bind(events_name)
        .bind(calls_name)
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
