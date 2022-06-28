use super::ArchiveService;
use super::selection::{CallSelection, CallDataSelection, EventSelection, EvmLogSelection, ContractsEventSelection};
use super::fields::{ExtrinsicFields, EventFields, CallFields};
use crate::entities::{Batch, Metadata, Status, Call, Event, BlockHeader, EvmLog, ContractsEvent, FullCall, Extrinsic};
use crate::error::Error;
use crate::metrics::ObserverExt;
use std::collections::HashMap;
use sqlx::{Pool, Postgres};
use utils::{unify_and_merge, merge};
use serializer::{CallSerializer, EventSerializer, ExtrinsicSerializer};

mod utils;
mod selection;
mod fields;
mod serializer;

struct CallInfo {
    pub fields: Vec<String>,
    pub parent: Option<Vec<String>>,
}

impl CallInfo {
    pub fn merge(&mut self, info: &CallInfo) {
        merge(&mut self.fields, &info.fields);
        if let Some(parent) = &mut self.parent {
            if let Some(other_parent) = &info.parent {
                merge(parent, &other_parent);
            }
        }
    }
}

struct CallGroup {
    call_info: CallInfo,
    ids: Vec<String>,
}

pub struct PostgresArchive {
    pool: Pool<Postgres>,
}

#[async_trait::async_trait]
impl ArchiveService for PostgresArchive {
    type EvmLogSelection = EvmLogSelection;
    type ContractsEventSelection = ContractsEventSelection;
    type EventSelection = EventSelection;
    type CallSelection = CallSelection;
    type Batch = Batch;
    type Metadata = Metadata;
    type Status = Status;
    type Error = Error;

    async fn batch(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        evm_log_selections: &Vec<Self::EvmLogSelection>,
        contracts_event_selections: &Vec<Self::ContractsEventSelection>,
        event_selections: &Vec<Self::EventSelection>,
        call_selections: &Vec<Self::CallSelection>,
        include_all_blocks: bool
    ) -> Result<Vec<Self::Batch>, Self::Error> {
        let mut calls = self.load_calls(limit, from_block, to_block, &call_selections).await?;
        let mut events = self.load_events(limit, from_block, to_block, &event_selections).await?;
        let mut evm_logs = self.get_evm_logs(limit, from_block, to_block, &evm_log_selections).await?;
        let mut contracts_events = self.get_contracts_events(limit, from_block, to_block,
                                                             &contracts_event_selections).await?;
        let blocks = if include_all_blocks {
            let blocks = self.get_blocks(from_block, to_block, limit).await?;
            blocks
        } else {
            let block_ids = self.get_block_ids(&calls, &events, &evm_logs, &contracts_events, limit);
            calls = calls.into_iter().filter(|call| block_ids.contains(&call.block_id)).collect();
            events = events.into_iter().filter(|event| block_ids.contains(&event.block_id)).collect();
            evm_logs = evm_logs.into_iter().filter(|log| block_ids.contains(&log.block_id)).collect();
            contracts_events = contracts_events.into_iter()
                .filter(|event| block_ids.contains(&event.block_id)).collect();
            self.get_blocks_by_ids(&block_ids).await?
        };

        let mut extrinsic_fields: HashMap<String, ExtrinsicFields> = HashMap::new();
        let mut event_fields: HashMap<String, EventFields> = HashMap::new();
        let mut call_fields: HashMap<String, CallDataSelection> = HashMap::new();

        for event in &events {
            for selection in event_selections {
                if selection.r#match(event) {
                    event_fields.entry(event.id.clone())
                        .and_modify(|fields| fields.merge(&selection.data.event))
                        .or_insert_with(|| selection.data.event.clone());

                    if let Some(extrinsic_id) = &event.extrinsic_id {
                        if selection.data.event.extrinsic.any() {
                            extrinsic_fields.entry(extrinsic_id.clone())
                                .and_modify(|fields| fields.merge(&selection.data.event.extrinsic))
                                .or_insert_with(|| selection.data.event.extrinsic.clone());
                        }
                    }
                }
            }
        }
        for event in &contracts_events {
            if let Some(value) = event.data.get("extrinsic_id") {
                if let Some(extrinsic_id) = value.as_str() {
                    for selection in contracts_event_selections {
                        if selection.r#match(event) {
                            if selection.data.event.extrinsic.any() {
                                extrinsic_fields.entry(extrinsic_id.to_string())
                                    .and_modify(|fields| fields.merge(&selection.data.event.extrinsic))
                                    .or_insert_with(|| selection.data.event.extrinsic.clone());
                            }
                        }
                    }
                }
            }
        }
        for log in &evm_logs {
            if let Some(value) = log.data.get("extrinsic_id") {
                if let Some(extrinsic_id) = value.as_str() {
                    let selection = &evm_log_selections[log.selection_index as usize];
                    if selection.data.event.extrinsic.any() {
                        extrinsic_fields.entry(extrinsic_id.to_string())
                            .and_modify(|fields| fields.merge(&selection.data.event.extrinsic))
                            .or_insert_with(|| selection.data.event.extrinsic.clone());
                    }
                }
            }
        }
        for call in &calls {
            for selection in call_selections {
                if selection.r#match(call) {
                    if let Some(fields) = call_fields.get_mut(&call.id) {
                        fields.call.merge(&selection.data.call);
                        fields.extrinsic.merge(&selection.data.extrinsic)
                    } else {
                        call_fields.insert(call.id.clone(), selection.data.clone());
                    }
                    if selection.data.extrinsic.any() {
                        if let Some(fields) = extrinsic_fields.get_mut(&call.extrinsic_id) {
                            fields.merge(&selection.data.extrinsic);
                        } else {
                            extrinsic_fields.insert(
                                call.extrinsic_id.clone(),
                                selection.data.extrinsic.clone()
                            );
                        }
                    }
                    self.visit_parent_call(&call, &selection.data, &calls, &mut call_fields);
                }
            }
        }

        let extrinsic_ids = extrinsic_fields.keys().cloned().collect();
        let extrinsics = self.load_extrinsics(&extrinsic_ids).await?;
        let mut extrinsics_by_block: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for extrinsic in extrinsics {
            let fields = extrinsic_fields.get(&extrinsic.id).unwrap();
            let serializer = ExtrinsicSerializer { extrinsic: &extrinsic, fields };
            let data = serde_json::to_value(serializer).unwrap();
            if extrinsics_by_block.contains_key(&extrinsic.block_id) {
                extrinsics_by_block.get_mut(&extrinsic.block_id).unwrap().push(data);
            } else {
                extrinsics_by_block.insert(extrinsic.block_id.clone(), vec![data]);
            }
        }

        let events_calls = self.get_events_calls(&event_selections, &events).await?;
        let evm_logs_calls = self.get_evm_logs_calls(&evm_log_selections, &evm_logs).await?;
        let contracts_events_calls = self.get_contracts_events_calls(
            &contracts_event_selections, &contracts_events).await?;

        let mut events_by_block: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for event in events {
            let fields = event_fields.get(&event.id).unwrap();
            let serializer = EventSerializer { event: &event, fields };
            let data = serde_json::to_value(serializer).unwrap();
            if events_by_block.contains_key(&event.block_id) {
                events_by_block.get_mut(&event.block_id).unwrap().push(data);
            } else {
                events_by_block.insert(event.block_id.clone(), vec![data]);
            }
        }

        let mut calls_by_block: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for call in calls {
            if let Some(fields) = call_fields.get(&call.id) {
                let serializer = CallSerializer { call: &call, fields };
                let data = serde_json::to_value(serializer).unwrap();
                if let Some(calls) = calls_by_block.get_mut(&call.block_id) {
                    calls.push(data);
                } else {
                    calls_by_block.insert(call.block_id.clone(), vec![data]);
                }
            }
        }
        let batch = self.create_batch(blocks, events_by_block, calls_by_block, extrinsics_by_block, events_calls,
                                      evm_logs, evm_logs_calls, contracts_events, contracts_events_calls);
        Ok(batch)
    }

    async fn metadata(&self) -> Result<Vec<Self::Metadata>, Self::Error> {
        let query = "SELECT id, spec_name, spec_version::int8, block_height::int8, block_hash, hex FROM metadata";
        let metadata = sqlx::query_as::<_, Metadata>(query)
            .fetch_all(&self.pool)
            .observe_duration("metadata")
            .await?;
        Ok(metadata)
    }

    async fn metadata_by_id(&self, id: String) -> Result<Option<Self::Metadata>, Self::Error> {
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
            .fetch_optional(&self.pool)
            .observe_duration("block")
            .await?
            .unwrap_or_else(|| Status { head: 0 });
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
        call_selections: &Vec<CallSelection>
    ) -> Result<Vec<FullCall>, Error> {
        if call_selections.is_empty() {
            return Ok(Vec::new());
        }
        let mut wildcard = false;
        let mut names = Vec::new();
        for selection in call_selections {
            if selection.name == "*" {
                wildcard = true;
            } else {
                names.push(selection.name.clone());
            }
        }
        let from_block = format!("{:010}", from_block);
        let to_block = to_block.map(|to_block| format!("{:010}", to_block + 1));
        let query = "WITH RECURSIVE recursive_calls AS (
                SELECT * FROM call WHERE ($1 OR name = ANY($2)) AND block_id IN (
                    SELECT DISTINCT block_id FROM call
                    WHERE ($1 OR name = ANY($2))
                        AND block_id >= $3 AND ($4 IS null OR block_id < $4)
                    GROUP BY block_id
                    ORDER BY block_id
                    LIMIT $5
                )
                UNION ALL
                SELECT DISTINCT call.*
                FROM call JOIN recursive_calls ON recursive_calls.parent_id = call.id
            )
            SELECT
                id,
                parent_id,
                block_id,
                extrinsic_id,
                name,
                args,
                success,
                error,
                origin,
                pos::int8
            FROM recursive_calls
            ORDER BY block_id";
        let calls = sqlx::query_as::<_, FullCall>(query)
            .bind(wildcard)
            .bind(names)
            .bind(from_block)
            .bind(to_block)
            .bind(limit)
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
        event_selections: &Vec<EventSelection>
    ) -> Result<Vec<Event>, Error> {
        if event_selections.is_empty() {
            return Ok(Vec::new());
        }
        let mut wildcard = false;
        let mut names = Vec::new();
        for selection in event_selections {
            if selection.name == "*" {
                wildcard = true;
            } else {
                names.push(selection.name.clone());
            }
        }
        let from_block = format!("{:010}", from_block);
        let to_block = to_block.map_or("null".to_string(), |to_block| format!("{:010}", to_block + 1));
        let query = "SELECT
                id,
                block_id,
                index_in_block::int8,
                phase,
                extrinsic_id,
                call_id,
                name,
                args,
                pos::int8,
                contract
            FROM event
            WHERE ($1 OR name = ANY($2)) AND block_id IN (
                SELECT DISTINCT block_id FROM event
                WHERE ($1 OR name = ANY($2))
                    AND block_id >= $3 AND ($4 IS null OR block_id < $4)
                GROUP BY block_id
                ORDER BY block_id
                LIMIT $5
            )";
        let events = sqlx::query_as::<_, Event>(&query)
            .bind(wildcard)
            .bind(names)
            .bind(from_block)
            .bind(to_block)
            .bind(limit)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn get_contracts_events(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        contracts_event_selections: &Vec<ContractsEventSelection>
    ) -> Result<Vec<ContractsEvent>, Error> {
        if contracts_event_selections.is_empty() {
            return Ok(Vec::new());
        }
        let query_dynamic_part = contracts_event_selections.iter()
            .enumerate()
            .map(|(index, selection)| {
                let mut selected_fields: Vec<String> = selection.data.event.selected_fields()
                    .iter()
                    .map(|field| field.to_string())
                    .collect();
                selected_fields.extend_from_slice(&[
                    "id".to_string(),
                    "pos".to_string(),
                    "name".to_string(),
                    "contract".to_string(),
                ]);
                let build_object_fields = selected_fields
                    .iter()
                    .map(|field| format!("'{}', event.{}", &field, &field))
                    .collect::<Vec<String>>()
                    .join(", ");
                let from_block = format!("{:010}", from_block);
                let to_block = to_block.map_or("null".to_string(), |to_block| format!("{:010}", to_block + 1));
                let contract_condition = if selection.contract == "*" {
                    "event.name = 'Contracts.ContractEmitted'".to_string()
                } else {
                    format!("event.contract = '{}'", &selection.contract)
                };
                format!("(
                    SELECT
                        event.block_id,
                        json_agg(jsonb_build_object(
                            'selection_index', {},
                            'data', jsonb_build_object({})
                        )) as events
                    FROM event
                    WHERE {} AND event.block_id >= '{}' AND ({} IS null OR event.block_id < '{}')
                    GROUP BY block_id
                    ORDER BY block_id
                    LIMIT {}
                )", index, &build_object_fields, &contract_condition, from_block, to_block, to_block, limit)
            })
            .collect::<Vec<String>>()
            .join(" UNION ");
        let query = format!("SELECT
                block_id,
                (json_array_elements(events) ->> 'selection_index')::INT2 AS selection_index,
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
            ) AS events_by_block", &query_dynamic_part, limit);
        let events = sqlx::query_as::<_, ContractsEvent>(&query)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn get_evm_logs(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        evm_log_selections: &Vec<EvmLogSelection>
    ) -> Result<Vec<EvmLog>, Error> {
        if evm_log_selections.is_empty() {
            return Ok(Vec::new());
        }
        let subqueries = evm_log_selections.iter()
            .enumerate()
            .map(|(index, selection)| {
                let mut selected_fields = selection.data.event.selected_fields();
                selected_fields.extend_from_slice(&["id".to_string(), "pos".to_string(), "name".to_string()]);
                let mut build_object_args: Vec<String> = selected_fields
                    .iter()
                    .map(|field| format!("'{}', event.{}", &field, &field))
                    .collect();
                if selection.data.event.evm_tx_hash {
                    // transaction_hash has second index
                    let tx_hash = "'evmTxHash', jsonb_extract_path_text(executed_event.args, '2')".to_string();
                    build_object_args.push(tx_hash);
                }
                let build_object_fields = build_object_args.join(", ");
                let from_block = format!("{:010}", from_block);
                let to_block = to_block.map_or("null".to_string(), |to_block| format!("{:010}", to_block + 1));
                let contract_condition = if selection.contract == "*" {
                    "event.name = 'EVM.Log'".to_string()
                } else {
                    format!("event.contract = '{}'", &selection.contract)
                };
                let mut filters = vec![contract_condition];
                let topics_filters: Vec<String> = selection.filter.iter()
                    .enumerate()
                    .filter_map(|(index, topics)| {
                        if topics.is_empty() {
                            return None
                        }
                        let topic_filter = topics.iter()
                            .map(|topic| {
                                format!("jsonb_extract_path_text(event.args, 'topics', '{}') = '{}'", index, topic)
                            })
                            .collect::<Vec<String>>()
                            .join(" OR ");
                        Some(format!("({})", topic_filter))
                    })
                    .collect();
                if !topics_filters.is_empty() {
                    let topics_filter = topics_filters.join(" AND ");
                    filters.push(format!("({})", topics_filter));
                }
                let conditions = filters.join(" AND ");
                format!("(
                    SELECT
                        event.block_id,
                        json_agg(
                            jsonb_build_object(
                                'selection_index', {},
                                'data', jsonb_build_object({})
                            )
                        ) AS logs
                    FROM event
                    INNER JOIN event executed_event
                        ON event.extrinsic_id = executed_event.extrinsic_id
                            AND executed_event.name = 'Ethereum.Executed'
                    WHERE {} AND event.block_id IN (
                        SELECT DISTINCT block_id FROM event
                        WHERE {} AND block_id >= '{}' AND ({} IS NULL OR block_id < '{}')
                        GROUP BY block_id
                        ORDER BY block_id
                        LIMIT {}
                    )
                    GROUP BY event.block_id
                )", index, &build_object_fields, &conditions, &conditions, from_block, to_block, to_block, limit)
            })
            .collect::<Vec<String>>()
            .join(" UNION ");
        let query = format!("SELECT
                block_id,
                (json_array_elements(logs) ->> 'selection_index')::INT2 AS selection_index,
                json_array_elements(logs) -> 'data' AS data
            FROM (
                SELECT
                    block_id,
                    json_agg(log) AS logs
                FROM (
                    SELECT
                        block_id,
                        json_array_elements(logs) as log
                    FROM ({}) AS logs
                ) AS logs
                GROUP BY block_id
                LIMIT {}
            ) AS logs_by_block", &subqueries, limit);
        let logs = sqlx::query_as::<_, EvmLog>(&query)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(logs)
    }

    fn get_block_ids(
        &self,
        calls: &Vec<FullCall>,
        events: &Vec<Event>,
        evm_logs: &Vec<EvmLog>,
        contracts_events: &Vec<ContractsEvent>,
        limit: i32
    ) -> Vec<String> {
        let mut block_ids = Vec::new();
        for call in calls {
            block_ids.push(call.block_id.clone());
        }
        for event in events {
            block_ids.push(event.block_id.clone());
        }
        for log in evm_logs {
            block_ids.push(log.block_id.clone());
        }
        for event in contracts_events {
            block_ids.push(event.block_id.clone());
        }
        block_ids.sort();
        block_ids.dedup();
        block_ids.truncate(limit as usize);
        block_ids
    }

    async fn get_blocks_by_ids(&self, ids: &Vec<String>) -> Result<Vec<BlockHeader>, Error> {
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

    async fn get_blocks(
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
            WHERE height >= $1 AND ($2 IS null OR height <= $2)
            ORDER BY height
            LIMIT $3";
        let blocks = sqlx::query_as::<_, BlockHeader>(&query)
            .bind(from_block)
            .bind(to_block)
            .bind(limit)
            .fetch_all(&self.pool)
            .observe_duration("block")
            .await?;
        Ok(blocks)
    }

    async fn load_extrinsics(&self, ids: &Vec<String>) -> Result<Vec<Extrinsic>, Error> {
        let query = "SELECT
                id,
                block_id,
                index_in_block::int8,
                version::int8,
                signature,
                success,
                error,
                call_id,
                fee,
                tip,
                hash,
                pos::int8
            FROM extrinsic WHERE id = ANY($1)";
        let extrinsics = sqlx::query_as::<_, Extrinsic>(&query)
            .bind(ids)
            .fetch_all(&self.pool)
            .observe_duration("extrinsic")
            .await?;
        Ok(extrinsics)
    }

    async fn get_events_calls(
        &self,
        event_selections: &Vec<EventSelection>,
        events: &Vec<Event>
    ) -> Result<Vec<Call>, Error> {
        let mut calls_info = HashMap::new();
        for event in events {
            let selections = event_selections.iter()
                .filter(|selection| selection.r#match(event));
            for selection in selections {
                if let Some(call_id) = &event.call_id {
                    if selection.data.event.call.any() {
                        let mut fields: Vec<String> = selection.data.event.call.selected_fields()
                            .iter()
                            .map(|field| field.to_string())
                            .collect();
                        fields.extend_from_slice(&["id".to_string(), "pos".to_string(), "name".to_string()]);
                        let parent = if selection.data.event.call.parent.any() {
                            let mut fields = selection.data.event.call.parent.selected_fields();
                            fields.extend_from_slice(&["id".to_string(), "pos".to_string(), "name".to_string()]);
                            Some(fields)
                        } else {
                            None
                        };
                        let info = CallInfo { fields, parent };
                        calls_info.entry(call_id.clone())
                            .and_modify(|call_info: &mut CallInfo| call_info.merge(&info))
                            .or_insert(info);
                    }
                }
            }
        }

        let mut groups: Vec<CallGroup> = Vec::new();
        for (call_id, call_info) in calls_info.drain() {
            if let Some(group) = groups.iter_mut().find(|group| group.call_info.fields == call_info.fields) {
                group.ids.push(call_id.to_string());
            } else {
                let group = CallGroup { call_info, ids: vec![call_id.to_string()] };
                groups.push(group);
            }
        }
        let query = groups.into_iter()
            .map(|group| {
                let build_object_fields = group.call_info.fields
                    .iter()
                    .map(|field| format!("'{}', {}", &field, &field))
                    .collect::<Vec<String>>()
                    .join(", ");
                let ids = group.ids.iter()
                    .map(|id| format!("'{}'", id))
                    .collect::<Vec<String>>()
                    .join(", ");
                if let Some(parent) = group.call_info.parent {
                    let parent_build_object_fields = parent
                        .iter()
                        .map(|field| format!("'{}', call.{}", &field, &field))
                        .collect::<Vec<String>>()
                        .join(", ");
                    format!("(WITH RECURSIVE child_call AS (
                        SELECT block_id, name, parent_id, jsonb_build_object({}) AS data FROM call WHERE id IN ({})
                        UNION ALL
                        SELECT call.block_id, call.name, call.parent_id, jsonb_build_object({}) AS data FROM call JOIN child_call ON child_call.parent_id = call.id
                    ) SELECT block_id, name, data FROM child_call)", &build_object_fields, &ids, &parent_build_object_fields)
                } else {
                    format!("(SELECT block_id, name, jsonb_build_object({}) as data FROM call WHERE id IN ({}))", &build_object_fields, &ids)
                }
            })
            .collect::<Vec<String>>()
            .join(" UNION ");
        let calls = sqlx::query_as::<_, Call>(&query)
            .fetch_all(&self.pool)
            .observe_duration("call")
            .await?;
        Ok(calls)
    }

    async fn get_contracts_events_calls(
        &self,
        contracts_event_selections: &Vec<ContractsEventSelection>,
        events: &Vec<ContractsEvent>
    ) -> Result<Vec<Call>, Error> {
        let mut calls_info = HashMap::new();
        for event in events {
            let selection = &contracts_event_selections[event.selection_index as usize];
            let mut call_fields: Vec<String> = selection.data.event.call.selected_fields()
                .iter()
                .map(|field| field.to_string())
                .collect();
            if !call_fields.is_empty() {
                call_fields.extend_from_slice(&["id".to_string(), "pos".to_string(), "name".to_string()]);
                let call_id = event.data.get("call_id")
                    .expect("call_id should be loaded").as_str();
                if let Some(call_id) = call_id {
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
            .observe_duration("call")
            .await?;
        Ok(calls)
    }

    async fn get_evm_logs_calls(
        &self,
        evm_log_selections: &Vec<EvmLogSelection>,
        evm_logs: &Vec<EvmLog>
    ) -> Result<Vec<Call>, Error> {
        let mut calls_info = HashMap::new();
        for log in evm_logs {
            let selection = &evm_log_selections[log.selection_index as usize];
            let mut fields: Vec<String> = selection.data.event.call.selected_fields()
                .iter()
                .map(|field| field.to_string())
                .collect();
            if !fields.is_empty() {
                fields.extend_from_slice(&["id".to_string(), "pos".to_string(), "name".to_string()]);
                let call_id = log.data.get("call_id")
                    .expect("call_id should be loaded").as_str().unwrap();
                calls_info.insert(call_id.clone(), fields);
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
            .observe_duration("call")
            .await?;
        Ok(calls)
    }

    fn create_batch(
        &self,
        blocks: Vec<BlockHeader>,
        mut events_by_block: HashMap<String, Vec<serde_json::Value>>,
        mut calls_by_block: HashMap<String, Vec<serde_json::Value>>,
        mut extrinsics_by_block: HashMap<String, Vec<serde_json::Value>>,
        events_calls: Vec<Call>,
        evm_logs: Vec<EvmLog>,
        evm_logs_calls: Vec<Call>,
        contracts_events: Vec<ContractsEvent>,
        contracts_events_calls: Vec<Call>,
    ) -> Vec<Batch> {
        for log in evm_logs {
            events_by_block.entry(log.block_id)
                .or_insert_with(Vec::new)
                .push(log.data);
        }
        for event in contracts_events {
            events_by_block.entry(event.block_id)
                .or_insert_with(Vec::new)
                .push(event.data);
        }
        for call in events_calls {
            calls_by_block.entry(call.block_id)
                .or_insert_with(Vec::new)
                .push(call.data);
        }
        for call in evm_logs_calls {
            calls_by_block.entry(call.block_id)
                .or_insert_with(Vec::new)
                .push(call.data);
        }
        for call in contracts_events_calls {
            calls_by_block.entry(call.block_id)
                .or_insert_with(Vec::new)
                .push(call.data);
        }
        blocks.into_iter()
            .map(|block| {
                let events = events_by_block.remove(&block.id).unwrap_or_default();
                let event_fields = vec!["id", "block_id", "index_in_block", "phase", "evmTxHash",
                                        "extrinsic_id", "call_id", "name", "args", "pos"];
                let deduplicated_events = unify_and_merge(events, event_fields);
                let calls = calls_by_block.remove(&block.id).unwrap_or_default();
                let call_fields = vec!["id", "parent_id", "block_id", "extrinsic_id", "success",
                                       "error", "origin", "name", "args", "pos"];
                let deduplicated_calls = unify_and_merge(calls, call_fields);
                Batch {
                    extrinsics: extrinsics_by_block.remove(&block.id).unwrap_or_default(),
                    calls: deduplicated_calls,
                    events: deduplicated_events,
                    header: block,
                }
            })
            .collect()
    }

    fn visit_parent_call(
        &self,
        call: &FullCall,
        data: &CallDataSelection,
        calls: &Vec<FullCall>,
        call_fields: &mut HashMap<String, CallDataSelection>,
    ) {
        if let Some(parent_id) = &call.parent_id {
            if data.call.parent.any() {
                let parent = calls.iter().find(|call| &call.id == parent_id)
                    .expect("parent call expected to be loaded");
                let parent_fields = CallDataSelection {
                    call: CallFields::from_parent(&data.call.parent),
                    extrinsic: ExtrinsicFields::new(false),
                };
                self.visit_parent_call(&parent, &parent_fields, calls, call_fields);
                if let Some(fields) = call_fields.get_mut(&parent.id) {
                    fields.call.merge(&parent_fields.call);
                    fields.extrinsic.merge(&parent_fields.extrinsic);
                } else {
                    call_fields.insert(parent.id.clone(), parent_fields);
                }
            }
        }
    }
}
