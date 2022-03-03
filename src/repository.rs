use crate::entities::{Block, BlockHeader, Extrinsic, Call, Event, Metadata, Status};
use sqlx::{postgres::PgRow, Error, Pool, Postgres, Row};
use async_graphql::InputObject;


#[derive(InputObject, Clone)]
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


#[derive(InputObject, Clone)]
pub struct CallFields {
    #[graphql(name="_all")]
    _all: Option<bool>,
    id: Option<bool>,
    extrinsic: Option<ExtrinsicFields>,
    parent_id: Option<bool>,
    parent: Option<bool>,
    success: Option<bool>,
    name: Option<bool>,
    args: Option<bool>,
}


#[derive(InputObject, Clone)]
pub struct EventFields {
    #[graphql(name="_all")]
    _all: Option<bool>,
    id: Option<bool>,
    block_id: Option<bool>,
    index_in_block: Option<bool>,
    phase: Option<bool>,
    extrinsic: Option<ExtrinsicFields>,
    call_id: Option<bool>,
    call: Option<CallFields>,
    name: Option<bool>,
    args: Option<bool>,
}


#[derive(InputObject, Clone)]
pub struct EventSelection {
    name: String,
    fields: EventFields,
}


#[derive(InputObject, Clone)]
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
    let events_name: Option<Vec<String>> = events.and_then(|events| {
        Some(events.iter().map(|selection| selection.name.clone()).collect())
    });
    let calls_name: Option<Vec<String>> = calls.and_then(|calls| {
        Some(calls.iter().map(|selection| selection.name.clone()).collect())
    });
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


pub async fn get_extrinsics(
    pool: &Pool<Postgres>,
    blocks: &[String],
    call_selections: &Option<Vec<CallSelection>>,
    event_selections: &Option<Vec<EventSelection>>,
) -> Result<Vec<Extrinsic>, Error> {
    let calls_name = call_selections.as_ref().and_then(|call_selections| {
        Some(call_selections.iter()
            .filter_map(|selection| {
                if let Some(all) = &selection.fields._all {
                    if *all {
                        return Some(selection.name.clone());
                    }
                }
                if let Some(_extrinsic) = &selection.fields.extrinsic {
                    return Some(selection.name.clone());
                }
                None
            })
            .collect::<Vec<String>>())
    });
    let events_name = event_selections.as_ref().and_then(|event_selections| {
        Some(event_selections.iter()
            .filter_map(|selection| {
                // TODO: handle EventSelection.fields.call.extrinsic
                if let Some(all) = &selection.fields._all {
                    if *all {
                        return Some(selection.name.clone());
                    }
                }
                if let Some(_extrinsic) = &selection.fields.extrinsic {
                    return Some(selection.name.clone());
                }
                None
            })
            .collect::<Vec<String>>())
    });
    let query = "SELECT
            id,
            block_id,
            index_in_block,
            name,
            signature,
            success,
            hash
        FROM extrinsic
        WHERE block_id = ANY($1::char(16)[])
            AND (
                EXISTS (SELECT 1 FROM call WHERE call.extrinsic_id = extrinsic.id AND call.name = ANY($2))
                OR EXISTS (SELECT 1 FROM event WHERE event.extrinsic_id = extrinsic.id AND event.name = ANY($3))
            )";
    let extrinsics = sqlx::query_as::<_, Extrinsic>(query)
        .bind(blocks)
        .bind(calls_name)
        .bind(events_name)
        .fetch_all(pool)
        .await?;
    Ok(extrinsics)
}


pub async fn get_calls(
    pool: &Pool<Postgres>,
    blocks: &[String],
    call_selections: &Option<Vec<CallSelection>>,
    event_selections: &Option<Vec<EventSelection>>,
) -> Result<Vec<Call>, Error> {
    let calls_name = call_selections.as_ref().and_then(|call_selections| {
        Some(call_selections.iter()
            .map(|selection| selection.name.clone())
            .collect::<Vec<String>>())
    });
    let events_name = event_selections.as_ref().and_then(|event_selections| {
        Some(event_selections.iter()
            .filter_map(|selection| {
                if let Some(_all) = &selection.fields._all {
                    return Some(selection.name.clone());
                }
                if let Some(_call) = &selection.fields.call {
                    return Some(selection.name.clone());
                }
                None
            })
            .collect::<Vec<String>>())
    });
    let query = "WITH RECURSIVE child_call AS (
            SELECT
                call.id,
                call.index,
                call.extrinsic_id,
                call.parent_id,
                call.success,
                call.name,
                call.args,
                extrinsic.block_id
            FROM call
            INNER JOIN extrinsic ON call.extrinsic_id = extrinsic.id
            WHERE extrinsic.block_id = ANY($1::char(16)[])
                AND (call.name = ANY($2) OR EXISTS (
                    SELECT 1 FROM event WHERE event.block_id = extrinsic.block_id AND event.name = ANY($3)
                ))
        UNION
            SELECT
                call.id,
                call.index,
                call.extrinsic_id,
                call.parent_id,
                call.success,
                call.name,
                call.args,
                child_call.block_id
            FROM call INNER JOIN child_call ON child_call.parent_id = call.id
        ) SELECT * FROM child_call";
    let calls = sqlx::query_as::<_, Call>(query)
        .bind(&blocks)
        .bind(&calls_name)
        .bind(&events_name)
        .fetch_all(pool)
        .await?;

    Ok(calls)
}


pub async fn get_events(
    pool: &Pool<Postgres>,
    blocks: &[String],
    event_selections: &Option<Vec<EventSelection>>
) -> Result<Vec<Event>, Error> {
    let events_name = event_selections.as_ref().and_then(|event_selections| {
        Some(event_selections.iter()
            .map(|selection| selection.name.clone())
            .collect::<Vec<String>>())
    });
    let query = "SELECT
            id,
            block_id,
            index_in_block,
            phase,
            extrinsic_id,
            call_id,
            name,
            args
        FROM event
        WHERE block_id = ANY($1::char(16)[]) AND name = ANY($2)";
    let events = sqlx::query_as::<_, Event>(query)
        .bind(blocks)
        .bind(events_name)
        .fetch_all(pool)
        .await?;
    Ok(events)
}


pub async fn get_metadata(pool: &Pool<Postgres>) -> Result<Vec<Metadata>, Error> {
    let query = "SELECT spec_version, block_height, block_hash, hex FROM metadata";
    let metadata = sqlx::query_as::<_, Metadata>(query)
        .fetch_all(pool)
        .await?;
    Ok(metadata)
}


pub async fn get_status(pool: &Pool<Postgres>) -> Result<Status, Error> {
    let query = "SELECT height as head FROM block ORDER BY height DESC LIMIT 1";
    let status = sqlx::query_as::<_, Status>(query)
        .fetch_one(pool)
        .await?;
    Ok(status)
}
