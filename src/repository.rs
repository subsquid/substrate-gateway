use crate::entities::{Block, BlockHeader, Extrinsic, Call, Event, Metadata, Status};
use sqlx::{postgres::PgRow, Error, Pool, Postgres, Row};
use async_graphql::InputObject;


#[derive(InputObject, Clone)]
pub struct ExtrinsicFields {
    #[graphql(name="_all")]
    _all: Option<bool>,
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
    index: Option<bool>,
    block_id: Option<bool>,
    extrinsic: Option<ExtrinsicFields>,
    parent: Option<bool>,
    success: Option<bool>,
    name: Option<bool>,
    args: Option<bool>,
}


#[derive(InputObject, Clone)]
pub struct EventFields {
    #[graphql(name="_all")]
    _all: Option<bool>,
    block_id: Option<bool>,
    index_in_block: Option<bool>,
    phase: Option<bool>,
    extrinsic: Option<ExtrinsicFields>,
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
            timestamp,
            spec_version
        FROM block
        WHERE height >= $1
            AND ($2 IS null OR height < $2)
            AND ($3 IS true OR (
                EXISTS (SELECT 1 FROM event WHERE event.block_id = block.id AND event.name = ANY($4))
                OR EXISTS (SELECT 1 FROM call WHERE call.block_id = block.id AND call.name = ANY($5))
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
                spec_version: row.get_unchecked("spec_version"),
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
    let mut columns = get_extrinsics_columns(event_selections, call_selections);
    columns.push("block_id AS _block_id".to_string());
    let query = format!("SELECT
            {columns}
        FROM extrinsic
        WHERE block_id = ANY($1::char(16)[])
            AND (
                EXISTS (SELECT 1 FROM call WHERE call.extrinsic_id = extrinsic.id AND call.name = ANY($2))
                OR EXISTS (SELECT 1 FROM event WHERE event.extrinsic_id = extrinsic.id AND event.name = ANY($3))
            )",
        columns=columns.join(", ")
    );
    let extrinsics = sqlx::query_as::<_, Extrinsic>(&query)
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
    let mut columns = get_calls_columns(call_selections, event_selections);
    let mut child_columns = columns.clone();
    columns.push("call.block_id AS _block_id".to_string());
    columns.push("call.parent_id AS _parent_id".to_string());
    child_columns.push("child_call._block_id AS _block_id".to_string());
    child_columns.push("child_call._parent_id AS _parent_id".to_string());
    let query = format!("WITH RECURSIVE child_call AS (
            SELECT
                {columns}
            FROM call
            WHERE call.block_id = ANY($1::char(16)[]) AND call.name = ANY($2)
            UNION
            SELECT
                {columns}
            FROM call
            INNER JOIN extrinsic ON call.extrinsic_id = extrinsic.id
            WHERE extrinsic.block_id = ANY($1::char(16)[])
                AND EXISTS (SELECT 1 FROM event WHERE event.block_id = call.block_id AND event.name = ANY($3))
        UNION
            SELECT
                {child_columns}
            FROM call INNER JOIN child_call ON child_call._parent_id = call.id
        ) SELECT * FROM child_call",
        columns=columns.join(", "),
        child_columns=child_columns.join(", "),
    );
    let calls = sqlx::query_as::<_, Call>(&query)
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
    let mut columns = get_events_columns(event_selections);
    columns.push("block_id as _block_id".to_string());
    let query = format!("SELECT {columns}
        FROM event
        WHERE block_id = ANY($1::char(16)[]) AND name = ANY($2)",
        columns=columns.join(", "),
    );
    let events = sqlx::query_as::<_, Event>(&query)
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


fn get_calls_columns(
    call_selections: &Option<Vec<CallSelection>>,
    event_selections: &Option<Vec<EventSelection>>
) -> Vec<String> {
    let mut columns = vec!["call.id".to_string()];
    let mut push_column = |column_name: String| {
        if !columns.contains(&column_name) {
            columns.push(column_name);
        }
    };
    if let Some(call_selections) = call_selections {
        for selection in call_selections {
            if let Some(all) = selection.fields._all {
                if all {
                    push_column("call.index".to_string());
                    push_column("call.block_id".to_string());
                    push_column("call.extrinsic_id".to_string());
                    push_column("call.parent_id".to_string());
                    push_column("call.success".to_string());
                    push_column("call.name".to_string());
                    push_column("call.args".to_string());
                }
            }

            if let Some(index) = selection.fields.index {
                if index {
                    push_column("call.index".to_string());
                }
            }

            if let Some(block_id) = selection.fields.block_id {
                if block_id {
                    push_column("call.block_id".to_string());
                }
            }

            if let Some(extrinsic_fields) = &selection.fields.extrinsic {
                if let Some(all) = extrinsic_fields._all {
                    if all {
                        push_column("call.extrinsic_id".to_string());
                    }
                }

                if let Some(block_id) = extrinsic_fields.block_id {
                    if block_id {
                        push_column("call.extrinsic_id".to_string());
                    }
                }

                if let Some(index_in_block) = extrinsic_fields.index_in_block {
                    if index_in_block {
                        push_column("call.extrinsic_id".to_string());
                    }
                }

                if let Some(name) = extrinsic_fields.name {
                    if name {
                        push_column("call.extrinsic_id".to_string());
                    }
                }

                if let Some(signature) = extrinsic_fields.signature {
                    if signature {
                        push_column("call.extrinsic_id".to_string());
                    }
                }

                if let Some(success) = extrinsic_fields.success {
                    if success {
                        push_column("call.extrinsic_id".to_string());
                    }
                }

                if let Some(hash) = extrinsic_fields.hash {
                    if hash {
                        push_column("call.extrinsic_id".to_string());
                    }
                }
            }

            if let Some(parent) = selection.fields.parent {
                if parent {
                    push_column("call.parent_id".to_string());
                }
            }

            if let Some(success) = selection.fields.success {
                if success {
                    push_column("call.success".to_string());
                }
            }

            if let Some(name) = selection.fields.name {
                if name {
                    push_column("call.name".to_string());
                }
            }

            if let Some(args) = selection.fields.args {
                if args {
                    push_column("call.args".to_string());
                }
            }
        }
    }
    if let Some(event_selections) = event_selections {
        for selection in event_selections {
            if let Some(all) = selection.fields._all {
                if all {
                    push_column("call.index".to_string());
                    push_column("call.block_id".to_string());
                    push_column("call.extrinsic_id".to_string());
                    push_column("call.parent_id".to_string());
                    push_column("call.success".to_string());
                    push_column("call.name".to_string());
                    push_column("call.args".to_string());
                }
            }

            if let Some(call_fields) = &selection.fields.call {
                if let Some(all) = call_fields._all {
                    if all {
                        push_column("call.index".to_string());
                        push_column("call.block_id".to_string());
                        push_column("call.extrinsic_id".to_string());
                        push_column("call.parent_id".to_string());
                        push_column("call.success".to_string());
                        push_column("call.name".to_string());
                        push_column("call.args".to_string());
                    }
                }

                if let Some(index) = call_fields.index {
                    if index {
                        push_column("call.index".to_string());
                    }
                }

                if let Some(block_id) = call_fields.block_id {
                    if block_id {
                        push_column("call.block_id".to_string());
                    }
                }

                if let Some(extrinsic_fields) = &call_fields.extrinsic {
                    if let Some(all) = extrinsic_fields._all {
                        if all {
                            push_column("call.extrinsic_id".to_string());
                        }
                    }

                    if let Some(block_id) = extrinsic_fields.block_id {
                        if block_id {
                            push_column("call.extrinsic_id".to_string());
                        }
                    }

                    if let Some(index_in_block) = extrinsic_fields.index_in_block {
                        if index_in_block {
                            push_column("call.extrinsic_id".to_string());
                        }
                    }

                    if let Some(name) = extrinsic_fields.name {
                        if name {
                            push_column("call.extrinsic_id".to_string());
                        }
                    }

                    if let Some(signature) = extrinsic_fields.signature {
                        if signature {
                            push_column("call.extrinsic_id".to_string());
                        }
                    }

                    if let Some(success) = extrinsic_fields.success {
                        if success {
                            push_column("call.extrinsic_id".to_string());
                        }
                    }

                    if let Some(hash) = extrinsic_fields.hash {
                        if hash {
                            push_column("call.extrinsic_id".to_string());
                        }
                    }
                }

                if let Some(parent) = call_fields.parent {
                    if parent {
                        push_column("call.parent_id".to_string());
                    }
                }

                if let Some(success) = call_fields.success {
                    if success {
                        push_column("call.success".to_string());
                    }
                }

                if let Some(name) = call_fields.name {
                    if name {
                        push_column("call.name".to_string());
                    }
                }

                if let Some(args) = call_fields.args {
                    if args {
                        push_column("call.args".to_string());
                    }
                }
            }
        }
    }
    columns
}


fn get_events_columns(event_selections: &Option<Vec<EventSelection>>) -> Vec<String> {
    let mut columns = vec!["id".to_string()];
    let mut push_column = |column_name: String| {
        if !columns.contains(&column_name) {
            columns.push(column_name);
        }
    };
    if let Some(event_selections) = event_selections {
        for selection in event_selections {
            if let Some(all) = selection.fields._all {
                if all {
                    push_column("block_id".to_string());
                    push_column("index_in_block".to_string());
                    push_column("phase".to_string());
                    push_column("extrinsic_id".to_string());
                    push_column("call_id".to_string());
                    push_column("name".to_string());
                    push_column("args".to_string());
                }
            }

            if let Some(block_id) = selection.fields.block_id {
                if block_id {
                    push_column("block_id".to_string());
                }
            }

            if let Some(index_in_block) = selection.fields.index_in_block {
                if index_in_block {
                    push_column("index_in_block".to_string());
                }
            }

            if let Some(phase) = selection.fields.phase {
                if phase {
                    push_column("phase".to_string());
                }
            }

            if let Some(extrinsic_fields) = &selection.fields.extrinsic {
                if let Some(all) = extrinsic_fields._all {
                    if all {
                        push_column("extrinsic_id".to_string());
                    }
                }

                if let Some(block_id) = extrinsic_fields.block_id {
                    if block_id {
                        push_column("extrinsic_id".to_string());
                    }
                }

                if let Some(index_in_block) = extrinsic_fields.index_in_block {
                    if index_in_block {
                        push_column("extrinsic_id".to_string());
                    }
                }

                if let Some(name) = extrinsic_fields.name {
                    if name {
                        push_column("extrinsic_id".to_string());
                    }
                }

                if let Some(signature) = extrinsic_fields.signature {
                    if signature {
                        push_column("extrinsic_id".to_string());
                    }
                }

                if let Some(success) = extrinsic_fields.success {
                    if success {
                        push_column("extrinsic_id".to_string());
                    }
                }

                if let Some(hash) = extrinsic_fields.hash {
                    if hash {
                        push_column("extrinsic_id".to_string());
                    }
                }
            }

            if let Some(call_fields) = &selection.fields.call {
                if let Some(all) = call_fields._all {
                    if all {
                        push_column("call_id".to_string());
                    }
                }

                if let Some(extrinsic_fields) = &call_fields.extrinsic {
                    if let Some(all) = extrinsic_fields._all {
                        if all {
                            push_column("call_id".to_string());
                        }
                    }

                    if let Some(block_id) = extrinsic_fields.block_id {
                        if block_id {
                            push_column("call_id".to_string());
                        }
                    }

                    if let Some(index_in_block) = extrinsic_fields.index_in_block {
                        if index_in_block {
                            push_column("call_id".to_string());
                        }
                    }

                    if let Some(name) = extrinsic_fields.name {
                        if name {
                            push_column("call_id".to_string());
                        }
                    }

                    if let Some(signature) = extrinsic_fields.signature {
                        if signature {
                            push_column("call_id".to_string());
                        }
                    }

                    if let Some(success) = extrinsic_fields.success {
                        if success {
                            push_column("call_id".to_string());
                        }
                    }

                    if let Some(hash) = extrinsic_fields.hash {
                        if hash {
                            push_column("call_id".to_string());
                        }
                    }
                }

                if let Some(parent) = call_fields.parent {
                    if parent {
                        push_column("call_id".to_string());
                    }
                }

                if let Some(success) = call_fields.success {
                    if success {
                        push_column("call_id".to_string());
                    }
                }

                if let Some(name) = call_fields.name {
                    if name {
                        push_column("call_id".to_string());
                    }
                }

                if let Some(args) = call_fields.args {
                    if args {
                        push_column("call_id".to_string());
                    }
                }
            }

            if let Some(name) = selection.fields.name {
                if name {
                    push_column("name".to_string());
                }
            }

            if let Some(args) = selection.fields.args {
                if args {
                    push_column("args".to_string());
                }
            }
        }
    }
    columns
}


fn get_extrinsics_columns(
    event_selections: &Option<Vec<EventSelection>>,
    call_selections: &Option<Vec<CallSelection>>
) -> Vec<String> {
    let mut columns = vec!["id".to_string()];
    let mut push_column = |column_name: String| {
        if !columns.contains(&column_name) {
            columns.push(column_name);
        }
    };
    if let Some(event_selections) = event_selections {
        for selection in event_selections {
            if let Some(all) = selection.fields._all {
                if all {
                    push_column("block_id".to_string());
                    push_column("index_in_block".to_string());
                    push_column("name".to_string());
                    push_column("signature".to_string());
                    push_column("success".to_string());
                    push_column("hash".to_string());
                }
            }

            if let Some(extrinsic_fields) = &selection.fields.extrinsic {
                if let Some(all) = extrinsic_fields._all {
                    if all {
                        push_column("block_id".to_string());
                        push_column("index_in_block".to_string());
                        push_column("name".to_string());
                        push_column("signature".to_string());
                        push_column("success".to_string());
                        push_column("hash".to_string());
                    }
                }

                if let Some(block_id) = extrinsic_fields.block_id {
                    if block_id {
                        push_column("block_id".to_string());
                    }
                }

                if let Some(index_in_block) = extrinsic_fields.index_in_block {
                    if index_in_block {
                        push_column("index_in_block".to_string());
                    }
                }

                if let Some(name) = extrinsic_fields.name {
                    if name {
                        push_column("name".to_string());
                    }
                }

                if let Some(signature) = extrinsic_fields.signature {
                    if signature {
                        push_column("signature".to_string());
                    }
                }

                if let Some(success) = extrinsic_fields.success {
                    if success {
                        push_column("success".to_string());
                    }
                }

                if let Some(hash) = extrinsic_fields.hash {
                    if hash {
                        push_column("hash".to_string());
                    }
                }
            }
        }
    }
    if let Some(call_selections) = call_selections {
        for selection in call_selections {
            if let Some(all) = selection.fields._all {
                if all {
                    push_column("block_id".to_string());
                    push_column("index_in_block".to_string());
                    push_column("name".to_string());
                    push_column("signature".to_string());
                    push_column("success".to_string());
                    push_column("hash".to_string());
                }
            }

            if let Some(extrinsic_fields) = &selection.fields.extrinsic {
                if let Some(all) = extrinsic_fields._all {
                    if all {
                        push_column("block_id".to_string());
                        push_column("index_in_block".to_string());
                        push_column("name".to_string());
                        push_column("signature".to_string());
                        push_column("success".to_string());
                        push_column("hash".to_string());
                    }
                }

                if let Some(block_id) = extrinsic_fields.block_id {
                    if block_id {
                        push_column("block_id".to_string());
                    }
                }

                if let Some(index_in_block) = extrinsic_fields.index_in_block {
                    if index_in_block {
                        push_column("index_in_block".to_string());
                    }
                }

                if let Some(name) = extrinsic_fields.name {
                    if name {
                        push_column("name".to_string());
                    }
                }

                if let Some(signature) = extrinsic_fields.signature {
                    if signature {
                        push_column("signature".to_string());
                    }
                }

                if let Some(success) = extrinsic_fields.success {
                    if success {
                        push_column("success".to_string());
                    }
                }

                if let Some(hash) = extrinsic_fields.hash {
                    if hash {
                        push_column("hash".to_string());
                    }
                }
            }
        }
    }
    columns
}
