use crate::entities::{BlockHeader, Extrinsic, Call, Event, Metadata, Status};
use crate::error::Error;
use sqlx::{Pool, Postgres};


pub async fn get_blocks(
    pool: &Pool<Postgres>,
    limit: i32,
    from_block: i32,
    to_block: Option<i32>,
    events: &Vec<String>,
    calls: &Vec<String>,
    include_all_blocks: bool
) -> Result<Vec<BlockHeader>, Error> {
    let query = "SELECT
            id,
            height,
            hash,
            parent_hash,
            timestamp,
            spec_id,
            validator
        FROM block
        WHERE height >= $1
            AND ($2 IS null OR height < $2)
            AND ($3 IS true OR (
                EXISTS (SELECT 1 FROM event WHERE event.block_id = block.id AND event.name = ANY($4))
                OR EXISTS (SELECT 1 FROM call WHERE call.block_id = block.id AND call.name = ANY($5))
            ))
        ORDER BY height
        LIMIT $6";
    let blocks = sqlx::query_as::<_, BlockHeader>(query)
        .bind(from_block)
        .bind(to_block)
        .bind(include_all_blocks)
        .bind(events)
        .bind(calls)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(blocks)
}


pub async fn get_extrinsics(
    pool: &Pool<Postgres>,
    blocks: &Vec<String>,
    calls: &Vec<String>,
    events: &Vec<String>,
) -> Result<Vec<Extrinsic>, Error> {
    let query = format!("SELECT
            id,
            block_id,
            index_in_block,
            signature,
            success,
            call_id,
            hash,
            pos,
            call_id as _call_id
        FROM extrinsic
        WHERE block_id = ANY($1::char(16)[])
            AND (
                EXISTS (SELECT 1 FROM call WHERE call.extrinsic_id = extrinsic.id AND call.name = ANY($2))
                OR EXISTS (SELECT 1 FROM event WHERE event.extrinsic_id = extrinsic.id AND event.name = ANY($3))
            )",
    );
    let extrinsics = sqlx::query_as::<_, Extrinsic>(&query)
        .bind(blocks)
        .bind(calls)
        .bind(events)
        .fetch_all(pool)
        .await?;
    Ok(extrinsics)
}


pub async fn get_calls(
    pool: &Pool<Postgres>,
    blocks: &Vec<String>,
    call_names: &Vec<String>,
    event_names: &Vec<String>,
) -> Result<Vec<Call>, Error> {
    let query = format!("WITH RECURSIVE child_call AS (
            SELECT
                call.id,
                call.block_id,
                call.extrinsic_id,
                call.parent_id,
                call.success,
                call.name,
                call.args,
                call.pos,
                call.name as _name,
                call.extrinsic_id as _extrinsic_id
            FROM call
            WHERE call.block_id = ANY($1::char(16)[]) AND call.name = ANY($2)
            UNION
            SELECT
                call.id,
                call.block_id,
                call.extrinsic_id,
                call.parent_id,
                call.success,
                call.name,
                call.args,
                call.pos,
                call.name as _name,
                call.extrinsic_id as _extrinsic_id
            FROM call
            INNER JOIN extrinsic ON call.extrinsic_id = extrinsic.id
            WHERE extrinsic.block_id = ANY($1::char(16)[])
                AND EXISTS (SELECT 1 FROM event WHERE event.block_id = call.block_id AND event.name = ANY($3))
        UNION
            SELECT
                call.id,
                call.block_id,
                call.extrinsic_id,
                call.parent_id,
                call.success,
                call.name,
                call.args,
                call.pos,
                call.name as _name,
                call.extrinsic_id as _extrinsic_id
            FROM call INNER JOIN child_call ON child_call.parent_id = call.id
        ) SELECT * FROM child_call",
    );
    let calls = sqlx::query_as::<_, Call>(&query)
        .bind(&blocks)
        .bind(&call_names)
        .bind(&event_names)
        .fetch_all(pool)
        .await?;

    Ok(calls)
}


pub async fn get_calls_by_ids(pool: &Pool<Postgres>, ids: &Vec<String>) -> Result<Vec<Call>, Error> {
    let query = "
        SELECT
            id,
            block_id,
            extrinsic_id,
            parent_id,
            success,
            name,
            args,
            pos
        FROM call
        WHERE id = ANY($1::varchar(30)[])
    ";
    let calls = sqlx::query_as::<_, Call>(&query)
        .bind(ids)
        .fetch_all(pool)
        .await?;
    Ok(calls)
}


pub async fn get_events(
    pool: &Pool<Postgres>,
    blocks: &Vec<String>,
    names: &Vec<String>
) -> Result<Vec<Event>, Error> {
    let query = format!("SELECT
            id,
            block_id,
            index_in_block,
            phase,
            extrinsic_id,
            call_id,
            name,
            args,
            pos
        FROM event
        WHERE block_id = ANY($1::char(16)[]) AND name = ANY($2)",
    );
    let events = sqlx::query_as::<_, Event>(&query)
        .bind(blocks)
        .bind(names)
        .fetch_all(pool)
        .await?;
    Ok(events)
}


pub async fn get_metadata(pool: &Pool<Postgres>) -> Result<Vec<Metadata>, Error> {
    let query = "SELECT id, spec_name, spec_version, block_height, block_hash, hex FROM metadata";
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
