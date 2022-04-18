use super::ArchiveService;
use super::selection::{CallSelection, EventSelection};
use crate::entities::{Batch, Metadata, Status, Call, Event, Extrinsic, BlockHeader};
use crate::error::Error;
use std::collections::HashMap;
use sqlx::{Pool, Postgres};


pub struct CockroachArchive {
    pool: Pool<Postgres>,
}

#[async_trait::async_trait]
impl ArchiveService for CockroachArchive {
    async fn batch(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        event_selections: &Vec<EventSelection>,
        call_selections: &Vec<CallSelection>,
        include_all_blocks: bool
    ) -> Result<Vec<Batch>, Error> {
        let calls = self.get_calls(limit, from_block, to_block, &call_selections).await?;
        let events = self.get_events(limit, from_block, to_block, &event_selections).await?;
        let block_ids = self.get_block_ids(&calls, &events, limit);
        let blocks = self.get_blocks_by_ids(&block_ids).await?;
        let extrinsics = self.get_extrinsics(&event_selections, &call_selections, &block_ids, &events, &calls).await?;
        let events_calls = self.get_events_calls(&event_selections, &block_ids, &events).await?;
        let batch = self.create_batch(blocks, events, calls, extrinsics, events_calls);
        Ok(batch) 
    }

    async fn metadata(&self) -> Result<Vec<Metadata>, Error> {
        let query = "SELECT id, spec_name, spec_version, block_height, block_hash, hex FROM metadata";
        let metadata = sqlx::query_as::<_, Metadata>(query)
            .fetch_all(&self.pool)
            .await?;
        Ok(metadata)
    }

    async fn status(&self) -> Result<Status, Error> {
        let query = "SELECT height as head FROM block ORDER BY height DESC LIMIT 1";
        let status = sqlx::query_as::<_, Status>(query)
            .fetch_one(&self.pool)
            .await?;
        Ok(status)
    }
}


impl CockroachArchive {
    pub fn new(pool: Pool<Postgres>) -> CockroachArchive {
        CockroachArchive { pool }
    }

    async fn get_calls(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        call_selections: &Vec<CallSelection>
    ) -> Result<Vec<Call>, Error> {
        if call_selections.is_empty() {
            return Ok(Vec::new());
        }
        let query_dynamic_part = call_selections.iter()
            .map(|selection| {
                if selection.data.call.parent.any() {
                    let mut selected_fields = selection.data.selected_fields();
                    selected_fields.push("id".to_string());
                    selected_fields.push("pos".to_string());
                    let mut parent_selected_fields = selection.data.call.parent.selected_fields();
                    parent_selected_fields.push("id".to_string());
                    parent_selected_fields.push("pos".to_string());
                    // TODO: control parents loading
                    let build_object_fields = selected_fields
                        .iter()
                        .map(|field| format!("'{}', call.{}", &field, &field))
                        .collect::<Vec<String>>()
                        .join(", ");
                    let parent_build_object_fields = parent_selected_fields
                        .iter()
                        .map(|field| format!("'{}', call.{}", &field, &field))
                        .collect::<Vec<String>>()
                        .join(", ");
                    let to_block = to_block.map_or("null".to_string(), |to_block| to_block.to_string());
                    format!("(
                        SELECT
                            block_id,
                            json_agg(call) as calls
                        FROM (
                            WITH RECURSIVE child_call AS (
                                SELECT
                                    id,
                                    block_id,
                                    parent_id,
                                    jsonb_build_object(
                                        'name', name,
                                        'data', jsonb_build_object({})
                                    ) AS call
                                FROM call
                                WHERE id in (
                                    SELECT unnest(calls)
                                    FROM (
                                        SELECT
                                            call.block_id,
                                            array_agg(call.id) AS calls
                                        FROM call
                                        INNER JOIN block
                                        ON call.block_id = block.id
                                        WHERE call.name = '{}' AND block.height >= {} AND ({} IS null OR block.height < {})
                                        GROUP BY call.block_id
                                        LIMIT {}
                                    )
                                )
                            UNION ALL
                                SELECT
                                    call.id,
                                    call.block_id,
                                    call.parent_id,
                                    jsonb_build_object(
                                        'name', call.name,
                                        'data', jsonb_build_object({})
                                    ) AS call
                                FROM call INNER JOIN child_call
                                ON child_call.parent_id = call.id
                            ) SELECT DISTINCT * FROM child_call
                        )
                        GROUP BY block_id
                    )", &build_object_fields, &selection.name, from_block, to_block, to_block, limit, &parent_build_object_fields)
                } else {
                    let mut selected_fields = selection.data.selected_fields();
                    selected_fields.push("id".to_string());
                    selected_fields.push("pos".to_string());
                    let build_object_fields = selected_fields
                        .iter()
                        .map(|field| format!("'{}', call.{}", &field, &field))
                        .collect::<Vec<String>>()
                        .join(", ");
                    let to_block = to_block.map_or("null".to_string(), |to_block| to_block.to_string());
                    format!("(
                        SELECT
                            call.block_id,
                            json_agg(jsonb_build_object(
                                'name', call.name,
                                'data', jsonb_build_object({})
                            )) as calls
                        FROM call
                        INNER JOIN block
                        ON call.block_id = block.id
                        WHERE call.name = '{}' AND block.height >= {} AND ({} IS null OR block.height < {})
                        GROUP BY call.block_id
                        LIMIT {}
                    )", &build_object_fields, &selection.name, from_block, to_block, to_block, limit)
                }
            })
            .collect::<Vec<String>>()
            .join(" UNION ");
        let query = format!("SELECT
                block_id,
                json_array_elements(calls) ->> 'name'::STRING as name,
                json_array_elements(calls) -> 'data' as data
            FROM (
                SELECT
                    block_id,
                    json_agg(call) AS calls
                FROM (
                    SELECT
                        block_id,
                        json_array_elements(calls) AS call
                    FROM ({}) AS calls_by_block
                ) AS calls
                GROUP BY block_id
                LIMIT {}
            )", &query_dynamic_part, limit);
        let calls = sqlx::query_as::<_, Call>(&query)
            .fetch_all(&self.pool)
            .await?;
        Ok(calls)
    }

    async fn get_events(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        event_selections: &Vec<EventSelection>
    ) -> Result<Vec<Event>, Error> {
        if event_selections.is_empty() {
            return Ok(Vec::new());
        }
        let query_dynamic_part = event_selections.iter()
            .map(|selection| {
                let mut selected_fields = selection.data.event.selected_fields();
                selected_fields.push("id".to_string());
                selected_fields.push("pos".to_string());
                let build_object_fields = selected_fields
                    .iter()
                    .map(|field| format!("'{}', event.{}", &field, &field))
                    .collect::<Vec<String>>()
                    .join(", ");
                let to_block = to_block.map_or("null".to_string(), |to_block| to_block.to_string());
                // TODO: add order by to calls?
                format!("(
                    SELECT
                        event.block_id,
                        json_agg(jsonb_build_object(
                            'name', event.name,
                            'data', jsonb_build_object({})
                        )) as events
                    FROM event
                    INNER JOIN block
                    ON event.block_id = block.id
                    WHERE event.name = '{}' AND block.height >= {} AND ({} IS null OR block.height < {})
                    GROUP BY block_id
                    ORDER BY block_id
                    LIMIT {}
                )", &build_object_fields, &selection.name, from_block, to_block, to_block, limit)
            })
            .collect::<Vec<String>>()
            .join(" UNION ");
        let query = format!("SELECT
                block_id,
                json_array_elements(events) ->> 'name'::STRING AS name,
                json_array_elements(events) -> 'data' AS data
            FROM (
                SELECT
                    block_id,
                    json_agg(event) AS events
                FROM (
                    SELECT
                        block_id,
                        json_array_elements(events) AS event
                    FROM ({}) AS events_by_block
                ) AS events
                GROUP BY block_id
                LIMIT {}
            )", &query_dynamic_part, limit);
        let events = sqlx::query_as::<_, Event>(&query)
            .fetch_all(&self.pool)
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

    pub async fn get_blocks_by_ids(&self, ids: &Vec<String>) -> Result<Vec<BlockHeader>, Error> {
        let query = "SELECT
                id,
                height,
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
            .await?;
        Ok(blocks)
    }

    async fn get_extrinsics(
        &self,
        event_selections: &Vec<EventSelection>,
        call_selections: &Vec<CallSelection>,
        block_ids: &Vec<String>,
        events: &Vec<Event>,
        calls: &Vec<Call>,
    ) -> Result<Vec<Extrinsic>, Error> {
        let mut extrinsics_info = HashMap::new();
        for event in events {
            if block_ids.contains(&event.block_id) {
                let selection = event_selections.iter()
                    .find(|selection| selection.name == event.name)
                    .unwrap();
                let mut extrinsic_fields = selection.data.event.extrinsic.selected_fields();
                if !extrinsic_fields.is_empty() {
                    extrinsic_fields.push("id".to_string());
                    extrinsic_fields.push("pos".to_string());
                    let extrinsic_id = event.data.get("extrinsic_id")
                        .expect("extrinsic_id should be loaded").as_str().unwrap();
                    // TODO: merge requirements from multiple events
                    extrinsics_info.insert(extrinsic_id.clone(), extrinsic_fields);
                }
            }
        }
        for call in calls {
            if block_ids.contains(&call.block_id) {
                let selection = call_selections.iter()
                    .find(|selection| selection.name == call.name);
                if let Some(selection) = selection {  // direct call
                    let mut fields = selection.data.extrinsic.selected_fields();
                    if !fields.is_empty() {
                        fields.push("id".to_string());
                        fields.push("pos".to_string());
                        let extrinsic_id = call.data.get("extrinsic_id")
                            .expect("extrinsic_id should be loaded").as_str().unwrap();
                        extrinsics_info.insert(extrinsic_id.clone(), fields);
                    }
                }
            }
        }
    
        let query = extrinsics_info.into_iter()
            .map(|(key, value)| {
                let build_object_fields = value
                    .iter()
                    .map(|field| format!("'{}', {}", &field, &field))
                    .collect::<Vec<String>>()
                    .join(", ");
                format!("(SELECT block_id, jsonb_build_object({}) as data FROM extrinsic WHERE id = '{}')", &build_object_fields, &key)
            })
            .collect::<Vec<String>>()
            .join(" UNION ");
        let extrinsics = sqlx::query_as::<_, Extrinsic>(&query)
            .fetch_all(&self.pool)
            .await?;
        Ok(extrinsics)
    }
    
    
    async fn get_events_calls(
        &self,
        event_selections: &Vec<EventSelection>,
        block_ids: &Vec<String>,
        events: &Vec<Event>
    ) -> Result<Vec<Call>, Error> {
        let mut calls_info = HashMap::new();
        for event in events {
            if block_ids.contains(&event.block_id) {
                let selection = event_selections.iter()
                    .find(|selection| selection.name == event.name)
                    .unwrap();
                let mut call_fields = selection.data.event.call.selected_fields();
                if !call_fields.is_empty() {
                    call_fields.push("id".to_string());
                    call_fields.push("pos".to_string());
                    let call_id = event.data.get("call_id")
                        .expect("call_id should be loaded").as_str().unwrap();
                    // TODO: merge requirements from multiple events
                    calls_info.insert(call_id.clone(), call_fields);
                }
            }
        }
        // check intersection between already loaded calls
        let query = calls_info.into_iter()
            .map(|(key, value)| {
                let build_object_fields = value
                    .iter()
                    .map(|field| format!("'{}', {}", &field, &field))
                    .collect::<Vec<String>>()
                    .join(", ");
                format!("(SELECT block_id, name, jsonb_build_object({}) as data FROM call WHERE id = '{}')", &build_object_fields, &key)
            })
            .collect::<Vec<String>>()
            .join(" UNION ");
        let calls = sqlx::query_as::<_, Call>(&query)
            .fetch_all(&self.pool)
            .await?;
        Ok(calls)
    }
    
    
    fn create_batch(
        &self,
        blocks: Vec<BlockHeader>,
        events: Vec<Event>,
        calls: Vec<Call>,
        extrinsics: Vec<Extrinsic>,
        events_calls: Vec<Call>
    ) -> Vec<Batch> {
        let mut events_by_block = HashMap::new();
        for event in events {
            events_by_block.entry(event.block_id)
                .or_insert_with(Vec::new)
                .push(event.data);
        }
        let mut extrinsics_by_block = HashMap::new();
        for extrinsic in extrinsics {
            extrinsics_by_block.entry(extrinsic.block_id.clone())
                .or_insert_with(Vec::new)
                .push(extrinsic.data);
        }
        let mut calls_by_block = HashMap::new();
        for call in calls {
            calls_by_block.entry(call.block_id)
                .or_insert_with(Vec::new)
                .push(call.data)
        }
        for call in events_calls {
            calls_by_block.entry(call.block_id)
                .or_insert_with(Vec::new)
                .push(call.data);
        }
        blocks.into_iter()
            .map(|block| {
                Batch {
                    extrinsics: extrinsics_by_block.remove(&block.id).unwrap_or_default(),
                    calls: calls_by_block.remove(&block.id).unwrap_or_default(),
                    events: events_by_block.remove(&block.id).unwrap_or_default(),
                    header: block,
                }
            })
            .collect()
    }    
}
