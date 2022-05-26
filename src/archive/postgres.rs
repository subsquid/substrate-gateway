use super::ArchiveService;
use super::selection::{CallSelection, EventSelection, EvmLogSelection, ContractsEventSelection};
use super::fields::{ExtrinsicFields, CallFields, ParentCallFields, EventFields};
use crate::entities::{Batch, Metadata, Status, BlockHeader};
use crate::error::Error;
use crate::metrics::ObserverExt;
use serde::Serialize;
use serde::ser::SerializeStruct;
use serde_json::Value;
use std::collections::HashMap;
use sqlx::{Pool, Postgres};

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct Call {
    pub id: String,
    pub parent_id: Option<String>,
    pub block_id: String,
    pub extrinsic_id: String,
    pub success: bool,
    pub name: String,
    pub args: Option<Value>,
    pub pos: i64,
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct Event {
    pub id: String,
    pub block_id: String,
    pub index_in_block: i64,
    pub phase: String,
    pub extrinsic_id: Option<String>,
    pub call_id: Option<String>,
    pub name: String,
    pub args: Option<Value>,
    pub pos: i64,
}

#[derive(sqlx::FromRow, Debug)]
pub struct Extrinsic {
    pub id: String,
    pub block_id: String,
    pub index_in_block: i64,
    pub signature: Option<Value>,
    pub success: bool,
    pub call_id: String,
    pub hash: String,
    pub pos: i64,
}

struct ExtrinsicSerializer<'a> {
    extrinsic: &'a Extrinsic,
    fields: &'a ExtrinsicFields,
}

impl<'a> Serialize for ExtrinsicSerializer<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer
    {
        let fields = self.fields.selected_fields();  // TODO: should return enum
        let mut state = serializer.serialize_struct("Extrinsic", fields.len() + 2)?;
        state.serialize_field("id", &self.extrinsic.id)?;
        state.serialize_field("pos", &self.extrinsic.pos)?;
        for field in fields {
            match field {
                "index_in_block" => state.serialize_field("index_in_block", &self.extrinsic.index_in_block)?,
                "signature" => state.serialize_field("signature", &self.extrinsic.signature)?,
                "success" => state.serialize_field("success", &self.extrinsic.success)?,
                "hash" => state.serialize_field("hash", &self.extrinsic.hash)?,
                "call_id" => state.serialize_field("call_id", &self.extrinsic.call_id)?,
                _ => panic!("unexpected field"),
            };
        }
        state.end()
    }
}

struct CallSerializer<'a> {
    call: &'a Call,
    fields: &'a CallFields,
}

impl<'a> Serialize for CallSerializer<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer
    {
        let fields = self.fields.selected_fields();  // TODO: should return enum
        let mut state = serializer.serialize_struct("Call", fields.len() + 2)?;
        state.serialize_field("id", &self.call.id)?;
        state.serialize_field("pos", &self.call.pos)?;
        for field in fields {
            match field {
                "success" => state.serialize_field("success", &self.call.success)?,
                "name" => state.serialize_field("name", &self.call.name)?,
                "args" => state.serialize_field("args", &self.call.args)?,
                "parent_id" => state.serialize_field("parent_id", &self.call.parent_id)?,
                _ => panic!("unexpected field"),
            };
        }
        state.end()
    }
}

struct EventSerializer<'a> {
    event: &'a Event,
    fields: &'a EventFields,
}

impl<'a> Serialize for EventSerializer<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer
    {
        let fields = self.fields.selected_fields();  // TODO: should return enum
        let mut state = serializer.serialize_struct("Event", fields.len() + 2)?;
        state.serialize_field("id", &self.event.id)?;
        state.serialize_field("pos", &self.event.pos)?;
        for field in fields {
            match field {
                "index_in_block" => state.serialize_field("index_in_block", &self.event.index_in_block)?,
                "phase" => state.serialize_field("phase", &self.event.phase)?,
                "extrinsic_id" => state.serialize_field("extrinsic_id", &self.event.extrinsic_id)?,
                "call_id" => state.serialize_field("call_id", &self.event.call_id)?,
                "name" => state.serialize_field("name", &self.event.name)?,
                "args" => state.serialize_field("args", &self.event.args)?,
                _ => panic!("unexpected field"),
            };
        }
        state.end()
    }
}

impl EventSelection {
    fn r#match(&self, event: &Event) -> bool {
        self.name == event.name
    }
}

impl CallSelection {
    fn r#match(&self, call: &Call) -> bool {
        self.name == call.name
    }
}

impl ParentCallFields {
    fn merge(&mut self, other: &ParentCallFields) {
        if other._all {
            self._all = true;
        }
        if other.name {
            self.name = true;
        }
        if other.args {
            self.args = true;
        }
        if other.success {
            self.success = true;
        }
        if other.parent {
            self.parent = true;
        }
    }
}

impl CallFields {
    fn merge(&mut self, other: &CallFields) {
        if other._all {
            self._all = true;
        }
        if other.success {
            self.success = true;
        }
        if other.name {
            self.name = true;
        }
        if other.args {
            self.args = true;
        }
        if other.parent.any() {
            self.parent.merge(&other.parent);
        }
    }
}

impl ExtrinsicFields {
    fn merge(&mut self, other: &ExtrinsicFields) {
        if other._all {
            self._all = true;
        }
        if other.index_in_block {
            self.index_in_block = true;
        }
        if other.signature {
            self.signature = true;
        }
        if other.success {
            self.success = true;
        }
        if other.hash {
            self.hash = true;
        }
        if other.call.any() {
            self.call.merge(&other.call);
        }
    }
}

impl EventFields {
    fn merge(&mut self, other: &EventFields) {
        if other._all {
            self._all = true;
        }
        if other.index_in_block {
            self.index_in_block = true;
        }
        if other.phase {
            self.phase = true;
        }
        if other.extrinsic.any() {
            self.extrinsic.merge(&other.extrinsic);
        }
        if other.call.any() {
            self.call.merge(&other.call);
        }
        if other.name {
            self.name = true;
        }
        if other.args {
            self.args = true;
        }
    }
}

pub struct PostgresArchive {
    pool: Pool<Postgres>,
}

#[async_trait::async_trait]
impl ArchiveService for PostgresArchive {
    async fn batch(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        evm_log_selections: &Vec<EvmLogSelection>,
        contracts_event_selections: &Vec<ContractsEventSelection>,
        event_selections: &Vec<EventSelection>,
        call_selections: &Vec<CallSelection>,
        include_all_blocks: bool
    ) -> Result<Vec<Batch>, Error> {
        let mut calls = self.load_calls(limit, from_block, to_block, &call_selections).await?;
        let mut events = self.load_events(limit, from_block, to_block, &event_selections,
                                          &evm_log_selections, &contracts_event_selections).await?;

        let blocks = if include_all_blocks {
            self.load_blocks(from_block, to_block, limit).await?
        } else {
            let block_ids = self.get_block_ids(&calls, &events, limit);
            calls.retain(|call| block_ids.contains(&call.block_id));
            events.retain(|event| block_ids.contains(&event.block_id));
            self.load_blocks_by_ids(&block_ids).await?
        };

        let mut calls_fields: HashMap<String, CallFields> = HashMap::new();
        let mut extrinsics_fields: HashMap<String, ExtrinsicFields> = HashMap::new();
        let mut events_fields: HashMap<String, EventFields> = HashMap::new();

        for call in &calls {
            let selection = call_selections.into_iter()
                .find(|selection| selection.r#match(call));
            if let Some(selection) = selection {
                let mut fields = CallFields::default();
                fields.merge(&selection.data.call);
                calls_fields.insert(call.id.clone(), fields);
            }
        }

        for event in &events {
            let mut event_fields = Vec::new();
            let mut extrinsic_fields = Vec::new();
            for selection in event_selections {
                if selection.r#match(event) {
                    event_fields.push(&selection.data.event);
                    if selection.data.event.extrinsic.any() {
                        extrinsic_fields.push(&selection.data.event.extrinsic);
                    }
                }
            }

            let mut fields = EventFields::default();
            for item in event_fields {
                fields.merge(item);
            }
            events_fields.insert(event.id.clone(), fields);

            if let Some(extrinsic_id) = &event.extrinsic_id {
                if !extrinsic_fields.is_empty() {
                    let fields = extrinsics_fields.entry(extrinsic_id.clone())
                        .or_insert_with(ExtrinsicFields::default);
                    for item in extrinsic_fields {
                        fields.merge(item);
                    }
                }
            }
        }

        let extrinsics_ids = extrinsics_fields.keys().cloned().collect();
        let extrinsics = self.load_extrinsics_by_ids(&extrinsics_ids).await?;
        let loaded_calls: Vec<String> = calls.iter()
            .map(|call| call.id.clone())
            .collect();
        let mut to_load = Vec::new();
        for extrinsic in &extrinsics {
            let fields = extrinsics_fields.get(&extrinsic.id)
                .expect("extrinsic fields expected to be set");
            if fields.call.any() && !loaded_calls.contains(&extrinsic.call_id) && !to_load.contains(&extrinsic.call_id) {
                to_load.push(extrinsic.call_id.clone());
            }
        }
        let related_calls = self.load_calls_by_ids(&to_load).await?;
        let mut extrinsics_by_block: HashMap<String, Vec<Value>> = HashMap::new();
        for extrinsic in extrinsics {
            let fields = extrinsics_fields.get(&extrinsic.id)
                .expect("extrinsic fields expected to be set");
            let serializer = ExtrinsicSerializer {
                extrinsic: &extrinsic,
                fields: &fields,
            };
            let data = serde_json::to_value(&serializer).expect("serialization failed");
            extrinsics_by_block.entry(extrinsic.block_id.clone())
                .or_insert_with(Vec::new)
                .push(data);
        }
        let mut calls_by_block: HashMap<String, Vec<Value>> = HashMap::new();
        for call in calls {
            let fields = calls_fields.get(&call.id);
            let data = if let Some(fields) = fields {
                let serializer = CallSerializer {
                    call: &call,
                    fields: &fields,
                };
                serde_json::to_value(&serializer).expect("serialization failed")
            } else {
                serde_json::to_value(&call).expect("serialization failed")
            };
            calls_by_block.entry(call.block_id.clone())
                .or_insert_with(Vec::new)
                .push(data);
        }
        let mut events_by_block: HashMap<String, Vec<Value>> = HashMap::new();
        for event in events {
            let fields = events_fields.get(&event.id);
            let data = if let Some(fields) = fields {
                let serializer = EventSerializer {
                    event: &event,
                    fields: &fields,
                };
                serde_json::to_value(&serializer).expect("serialization failed")
            } else {
                serde_json::to_value(&event).expect("serialization failed")
            };
            events_by_block.entry(event.block_id.clone())
                .or_insert_with(Vec::new)
                .push(data);
        }
        let batch = blocks.into_iter()
            .map(|block| {
                Batch {
                    extrinsics: extrinsics_by_block.remove(&block.id).unwrap_or_default(),
                    calls: calls_by_block.remove(&block.id).unwrap_or_default(),
                    events: events_by_block.remove(&block.id).unwrap_or_default(),
                    header: block,
                }
            })
            .collect();
        Ok(batch)
    }

    async fn metadata(&self) -> Result<Vec<Metadata>, Error> {
        let query = "SELECT id, spec_name, spec_version::int8, block_height::int8, block_hash, hex FROM metadata";
        let metadata = sqlx::query_as::<_, Metadata>(query)
            .fetch_all(&self.pool)
            .observe_duration("metadata")
            .await?;
        Ok(metadata)
    }

    async fn metadata_by_id(&self, id: String) -> Result<Option<Metadata>, Error> {
        let query = "SELECT id, spec_name, spec_version::int8, block_height::int8, block_hash, hex
            FROM metadata WHERE id = $1";
        let metadata = sqlx::query_as::<_, Metadata>(query)
            .bind(id)
            .fetch_optional(&self.pool)
            .observe_duration("metadata")
            .await?;
        Ok(metadata)
    }

    async fn status(&self) -> Result<Status, Error> {
        let query = "SELECT height::int8 as head FROM block ORDER BY height DESC LIMIT 1";
        let status = sqlx::query_as::<_, Status>(query)
            .fetch_one(&self.pool)
            .observe_duration("block")
            .await?;
        Ok(status)
    }
}


impl PostgresArchive {
    pub fn new(pool: Pool<Postgres>) -> PostgresArchive {
        PostgresArchive { pool }
    }

    async fn load_calls(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        call_selections: &Vec<CallSelection>,
    ) -> Result<Vec<Call>, Error> {
        if call_selections.is_empty() {
            return Ok(Vec::new());
        }
        let query_dynamic_part = call_selections.iter()
            .map(|selection| {
                let to_block = to_block.map_or("null".to_string(), |to_block| to_block.to_string());
                if selection.data.call.parent.any() {
                    format!("(WITH RECURSIVE child_call AS (
                        SELECT call.id, call.block_id, call.parent_id FROM call WHERE call.id IN (
                            SELECT unnest(calls) FROM (
                                SELECT call.block_id, array_agg(call.id) AS calls
                                FROM call JOIN block ON block.id = call.block_id
                                WHERE name = '{}' AND block.height >= {} AND ({} IS null OR block.height <= {})
                                GROUP BY call.block_id
                                LIMIT {}
                            ) AS calls_by_block
                        )
                        UNION
                        SELECT call.id, call.block_id, call.parent_id FROM call JOIN child_call ON child_call.parent_id = call.id
                    )
                    SELECT
                        child_call.block_id,
                        array_agg(child_call.id) AS calls
                    FROM child_call
                    GROUP BY child_call.block_id)", &selection.name, from_block, to_block, to_block, limit)
                } else {
                    format!("(
                        SELECT call.block_id, array_agg(call.id) AS calls
                        FROM call JOIN block ON block.id = call.block_id
                        WHERE name = '{}' AND block.height >= {} AND ({} IS null OR block.height <= {})
                        GROUP BY call.block_id
                        LIMIT {}
                    )", &selection.name, from_block, to_block, to_block, limit)
                }
            })
            .collect::<Vec<String>>()
            .join(" UNION ");
        let query = format!("SELECT * FROM call WHERE id IN (
            SELECT call_id
            FROM (
                SELECT unnest(calls) AS call_id
                FROM (
                    SELECT block_id, array_agg(call_id) AS calls
                    FROM (
                        SELECT block_id, unnest(calls) AS call_id
                        FROM ({}) AS calls_by_block
                    ) AS calls
                    GROUP BY block_id
                ) AS calls_by_block
            ) AS calls
        )", query_dynamic_part);
        let calls = sqlx::query_as::<_, Call>(&query)
            .fetch_all(&self.pool)
            .observe_duration("call")
            .await?;
        Ok(calls)
    }

    async fn load_calls_by_ids(&self, ids: &Vec<String>) -> Result<Vec<Call>, Error> {
        let query = "SELECT * FROM call WHERE id = ANY($1)";
        let calls = sqlx::query_as::<_, Call>(&query)
            .bind(ids)
            .fetch_all(&self.pool)
            .observe_duration("call")
            .await?;
        Ok(calls)
    }

    async fn load_events(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        event_selections: &Vec<EventSelection>,
        evm_log_selections: &Vec<EvmLogSelection>,
        contracts_event_selections: &Vec<ContractsEventSelection>,
    ) -> Result<Vec<Event>, Error> {
        if event_selections.is_empty() && evm_log_selections.is_empty() && contracts_event_selections.is_empty() {
            return Ok(Vec::new());
        }
        let to_block = to_block.map_or("null".to_string(), |to_block| to_block.to_string());
        let mut query_dynamic_parts = Vec::new();
        for selection in event_selections {
            let subquery = format!("(
                SELECT event.block_id, array_agg(event.id) AS events
                FROM event JOIN block ON block.id = event.block_id
                WHERE name = '{}' AND block.height >= {} AND ({} IS null OR block.height <= {})
                GROUP BY event.block_id
                LIMIT {}
            )", &selection.name, from_block, to_block, to_block, limit);
            query_dynamic_parts.push(subquery);
        }
        let query_dynamic_part = query_dynamic_parts.join(" UNION ");
        let query = format!("SELECT * FROM event WHERE id IN (
            SELECT event_id
            FROM (
                SELECT unnest(events) AS event_id
                FROM (
                    SELECT block_id, array_agg(event_id) AS events
                    FROM (
                        SELECT block_id, unnest(events) as event_id
                        FROM ({}) AS events_by_block
                    ) AS events
                    GROUP BY block_id
                ) AS events_by_block
            ) AS events
        )", query_dynamic_part);
        let events = sqlx::query_as::<_, Event>(&query)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    fn get_block_ids(&self, calls: &Vec<Call>, events: &Vec<Event>, limit: i32) -> Vec<String> {
        let mut block_ids = Vec::new();
        for call in calls {
            block_ids.push(call.block_id.clone());
        }
        for event in events {
            block_ids.push(event.block_id.clone());
        }
        block_ids.sort();
        block_ids.dedup();
        block_ids.truncate(limit as usize);
        block_ids
    }

    async fn load_blocks_by_ids(&self, ids: &Vec<String>) -> Result<Vec<BlockHeader>, Error> {
        let query = "SELECT
                id,
                height::int8,
                hash,
                parent_hash,
                timestamp,
                spec_id,
                validator
            FROM block
            WHERE id = ANY($1)";
        let blocks = sqlx::query_as::<_, BlockHeader>(&query)
            .bind(ids)
            .fetch_all(&self.pool)
            .observe_duration("block")
            .await?;
        Ok(blocks)
    }

    async fn load_blocks(
        &self,
        from_block: i32,
        to_block: Option<i32>,
        limit: i32,
    ) -> Result<Vec<BlockHeader>, Error> {
        let query = "SELECT
                id,
                height::int8,
                hash,
                parent_hash,
                timestamp,
                spec_id,
                validator
            FROM block
            WHERE height >= $1 AND ($2 IS null OR height <= $2) LIMIT $3";
        let blocks = sqlx::query_as::<_, BlockHeader>(&query)
            .bind(from_block)
            .bind(to_block)
            .bind(limit)
            .fetch_all(&self.pool)
            .observe_duration("block")
            .await?;
        Ok(blocks)
    }

    async fn load_extrinsics_by_ids(&self, ids: &Vec<String>) -> Result<Vec<Extrinsic>, Error> {
        let query = "SELECT * FROM extrinsic WHERE id = ANY($1)";
        let extrinsics = sqlx::query_as::<_, Extrinsic>(&query)
            .bind(ids)
            .fetch_all(&self.pool)
            .observe_duration("extrinsic")
            .await?;
        Ok(extrinsics)
    }
}
