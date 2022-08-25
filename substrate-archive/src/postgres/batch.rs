use std::collections::{HashMap, HashSet};
use sqlx::{Pool, Arguments};
use sqlx::postgres::{PgArguments, Postgres};
use crate::entities::{Batch, Event, BlockHeader, EvmLog, Call, Extrinsic};
use crate::error::Error;
use crate::fields::{ExtrinsicFields, EventFields, CallFields, EvmLogFields};
use crate::selection::{
    CallSelection, CallDataSelection, EventSelection,
    EvmLogSelection, ContractsEventSelection, EthTransactSelection,
    GearMessageEnqueuedSelection, GearUserMessageSentSelection,
    EventDataSelection, EvmExecutedSelection
};
use crate::metrics::ObserverExt;
use super::BatchOptions;
use super::utils::unify_and_merge;
use super::serializer::{CallSerializer, EventSerializer, ExtrinsicSerializer, EvmLogSerializer};

pub struct BatchLoader<'a> {
    pool: Pool<Postgres>,
    limit: i32,
    from_block: i32,
    to_block: Option<i32>,
    include_all_blocks: bool,
    call_selections: &'a Vec<CallSelection>,
    event_selections: &'a Vec<EventSelection>,
    evm_log_selections: &'a Vec<EvmLogSelection>,
    eth_transact_selections: &'a Vec<EthTransactSelection>,
    contracts_event_selections: &'a Vec<ContractsEventSelection>,
    gear_message_enqueued_selections: &'a Vec<GearMessageEnqueuedSelection>,
    gear_user_message_sent_selections: &'a Vec<GearUserMessageSentSelection>,
    evm_executed_selections: &'a Vec<EvmExecutedSelection>,
}

const EVENTS_BY_ID_QUERY: &str = "SELECT
        id,
        block_id,
        index_in_block::int8,
        phase,
        extrinsic_id,
        call_id,
        name,
        args,
        pos::int8
    FROM event
    WHERE id = ANY($1::char(23)[])";

impl<'a> BatchLoader<'a> {
    pub async fn load(&self) -> Result<Vec<Batch>, Error> {
        let mut calls = self.load_calls().await?;
        let mut events = self.load_events().await?;
        let mut evm_logs = self.load_evm_logs().await?;
        let mut eth_transactions = self.load_eth_transactions().await?;
        let mut contracts_events = self.load_contracts_events().await?;
        let mut messages_enqueued = self.load_messages_enqueued().await?;
        let mut messages_sent = self.load_messages_sent().await?;
        let mut evm_executed = self.load_evm_executed().await?;
        let blocks = if self.include_all_blocks {
            let blocks = self.get_blocks().await?;
            blocks
        } else {
            let block_ids = self.get_block_ids(&calls, &events, &evm_logs, &eth_transactions,
                                               &contracts_events, &messages_enqueued,
                                               &messages_sent, &evm_executed);
            calls = calls.into_iter().filter(|call| block_ids.contains(&call.block_id)).collect();
            events = events.into_iter().filter(|event| block_ids.contains(&event.block_id)).collect();
            evm_logs = evm_logs.into_iter().filter(|log| block_ids.contains(&log.block_id)).collect();
            eth_transactions = eth_transactions.into_iter()
                .filter(|transaction| block_ids.contains(&transaction.block_id)).collect();
            contracts_events = contracts_events.into_iter()
                .filter(|event| block_ids.contains(&event.block_id)).collect();
            messages_enqueued = messages_enqueued.into_iter()
                .filter(|event| block_ids.contains(&event.block_id)).collect();
            messages_sent = messages_sent.into_iter()
                .filter(|event| block_ids.contains(&event.block_id)).collect();
            evm_executed = evm_executed.into_iter()
                .filter(|event| block_ids.contains(&event.block_id)).collect();
            self.get_blocks_by_ids(&block_ids).await?
        };

        let mut extrinsic_fields: HashMap<String, ExtrinsicFields> = HashMap::new();
        let mut event_fields: HashMap<String, EventFields> = HashMap::new();
        let mut log_fields: HashMap<String, EvmLogFields> = HashMap::new();
        let mut call_fields: HashMap<String, CallDataSelection> = HashMap::new();

        let mut call_fields_to_load: HashMap<String, CallFields> = HashMap::new();

        for call in &eth_transactions {
            for selection in self.eth_transact_selections {
                if selection.r#match(call) {
                    if let Some(fields) = call_fields.get_mut(&call.id) {
                        fields.call.merge(&selection.data.call);
                        fields.extrinsic.merge(&selection.data.extrinsic);
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
        for call in &calls {
            for selection in self.call_selections {
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

        let mut process_event = |event: &Event, data: &EventDataSelection| {
            event_fields.entry(event.id.clone())
                .and_modify(|fields| fields.merge(&data.event))
                .or_insert_with(|| data.event.clone());

            if let Some(extrinsic_id) = &event.extrinsic_id {
                if data.event.extrinsic.any() {
                    extrinsic_fields.entry(extrinsic_id.clone())
                        .and_modify(|fields| fields.merge(&data.event.extrinsic))
                        .or_insert_with(|| data.event.extrinsic.clone());
                }
            }

            if let Some(call_id) = &event.call_id {
                if data.event.call.any() {
                    if let Some(fields) = call_fields.get_mut(call_id) {
                        fields.call.merge(&data.event.call);
                    } else {
                        if let Some(fields) = call_fields_to_load.get_mut(call_id) {
                            fields.merge(&data.event.call);
                        } else {
                            call_fields_to_load.insert(
                                call_id.clone(),
                                data.event.call.clone()
                            );
                        }
                    }
                }
            }
        };
        for event in &events {
            for selection in self.event_selections {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        for event in &messages_enqueued {
            for selection in self.gear_message_enqueued_selections {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut messages_enqueued);
        for event in &messages_sent {
            for selection in self.gear_user_message_sent_selections {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut messages_sent);
        for event in &evm_executed {
            for selection in self.evm_executed_selections {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut evm_executed);
        for event in &contracts_events {
            for selection in self.contracts_event_selections {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut contracts_events);

        for log in &evm_logs {
            for selection in self.evm_log_selections {
                if selection.r#match(log) {
                    log_fields.entry(log.id.clone())
                        .and_modify(|fields| fields.merge(&selection.data.event))
                        .or_insert_with(|| selection.data.event.clone());

                    if let Some(extrinsic_id) = &log.extrinsic_id {
                        if selection.data.event.extrinsic.any() {
                            extrinsic_fields.entry(extrinsic_id.to_string())
                                .and_modify(|fields| fields.merge(&selection.data.event.extrinsic))
                                .or_insert_with(|| selection.data.event.extrinsic.clone());
                        }
                    }
                    if let Some(call_id) = &log.call_id {
                        if let Some(fields) = call_fields.get_mut(call_id) {
                            fields.call.merge(&selection.data.event.call);
                        } else {
                            if let Some(fields) = call_fields_to_load.get_mut(call_id) {
                                fields.merge(&selection.data.event.call);
                            } else {
                                call_fields_to_load.insert(
                                    call_id.to_string(),
                                    selection.data.event.call.clone()
                                );
                            }
                        }
                    }
                }
            }
        }

        let extrinsic_ids = extrinsic_fields.keys().cloned().collect();
        let extrinsics = self.load_extrinsics(&extrinsic_ids).await?;

        for extrinsic in &extrinsics {
            let fields = extrinsic_fields.get(&extrinsic.id).unwrap();
            if fields.call.any() {
                if let Some(call_fields) = call_fields.get_mut(&extrinsic.call_id) {
                    call_fields.call.merge(&fields.call);
                } else {
                    if let Some(call_fields) = call_fields_to_load.get_mut(&extrinsic.call_id) {
                        call_fields.merge(&fields.call);
                    } else {
                        call_fields_to_load.insert(
                            extrinsic.call_id.clone(),
                            fields.call.clone()
                        );
                    }
                }
            }
        }

        if !call_fields_to_load.is_empty() {
            let call_ids = call_fields_to_load.keys().cloned().collect();
            let mut additional_calls = self.load_calls_by_ids(&call_ids).await?;
            for call in &additional_calls {
                if let Some(fields) = call_fields_to_load.remove(&call.id) {
                    let data_selection = CallDataSelection {
                        call: fields,
                        extrinsic: ExtrinsicFields::new(false),
                    };
                    self.visit_parent_call(call, &data_selection, &additional_calls, &mut call_fields);
                    call_fields.insert(call.id.clone(), data_selection);
                }
            }
            calls.append(&mut additional_calls);
        }

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

        let mut logs_by_block: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for log in evm_logs {
            let fields = log_fields.get(&log.id).unwrap();
            let serializer = EvmLogSerializer { log: &log, fields };
            let data = serde_json::to_value(serializer).unwrap();
            if logs_by_block.contains_key(&log.block_id) {
                logs_by_block.get_mut(&log.block_id).unwrap().push(data);
            } else {
                logs_by_block.insert(log.block_id.clone(), vec![data]);
            }
        }

        let mut calls_by_block: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        calls.append(&mut eth_transactions);
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
        let batch = self.create_batch(blocks, events_by_block, calls_by_block, extrinsics_by_block,
                                      logs_by_block);
        Ok(batch)
    }

    async fn load_calls(&self) -> Result<Vec<Call>, Error> {
        if self.call_selections.is_empty() {
            return Ok(Vec::new());
        }
        let wildcard = self.call_selections
            .iter()
            .any(|selection| selection.name == "*");
        let names = self.call_selections
            .iter()
            .map(|selection| selection.name.clone())
            .collect::<Vec<String>>();

        let from_block = format!("{:010}", self.from_block);
        let to_block = self.to_block.map(|to_block| format!("{:010}", to_block + 1));

        let build_args = |_last_id: Option<String>, len: usize, limit: i64| {
            let mut args = PgArguments::default();
            let mut sql = String::from("SELECT block_id
                FROM call
                WHERE block_id > $1");
            let mut args_len = 1;
            args.add(&from_block);
            if let Some(to_block) = &to_block {
                args_len += 1;
                args.add(to_block);
                sql.push_str(&format!(" AND block_id < ${}", args_len));
            }
            if !wildcard {
                args_len += 1;
                args.add(&names);
                sql.push_str(&format!(" AND name = ANY(${})", args_len));
            }
            sql.push_str(" ORDER BY block_id");
            args_len += 1;
            args.add(len as i64);
            sql.push_str(&format!(" OFFSET ${}", args_len));
            args_len += 1;
            args.add(limit);
            sql.push_str(&format!(" LIMIT ${}", args_len));

            (sql, args)
        };
        let chunk_limit = 5000;
        let ids = self.load_ids(build_args, "call", chunk_limit).await?;

        let mut query = String::from("SELECT
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
            FROM call WHERE block_id = ANY($1::char(16)[])");
        let mut args = PgArguments::default();
        args.add(&ids);
        if !wildcard {
            query.push_str(&format!(" AND name = ANY($2)"));
            args.add(&names)
        }
        let mut calls = sqlx::query_as_with::<_, Call, _>(&query, args)
            .fetch_all(&self.pool)
            .observe_duration("call")
            .await?;

        let query = "SELECT
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
            FROM call WHERE id = ANY($1::varchar(30)[])";
        let mut parents_ids: Vec<String> = calls.iter()
            .filter_map(|call| call.parent_id.clone())
            .collect();
        parents_ids.sort();
        parents_ids.dedup();
        while !parents_ids.is_empty() {
            let to_load: Vec<String> = parents_ids.iter()
                .filter_map(|parent_id| {
                    let loaded = calls.iter().any(|call| &call.id == parent_id);
                    if loaded {
                        None
                    } else {
                        Some(parent_id.clone())
                    }
                })
                .collect();
            if !to_load.is_empty() {
                let mut parents = sqlx::query_as::<_, Call>(query)
                    .bind(to_load)
                    .fetch_all(&self.pool)
                    .observe_duration("call")
                    .await?;
                calls.append(&mut parents);
            }
            parents_ids = parents_ids.iter()
                .filter_map(|parent_id| {
                    let call = calls.iter().find(|call| &call.id == parent_id).unwrap();
                    call.parent_id.clone()
                })
                .collect();
            parents_ids.sort();
            parents_ids.dedup();
        }
        Ok(calls)
    }

    async fn load_calls_by_ids(&self, ids: &Vec<String>) -> Result<Vec<Call>, Error> {
        // i inject ids into the query otherwise it executes so long
        let ids = ids.iter().map(|id| format!("'{}'", id))
            .collect::<Vec<String>>()
            .join(", ");
        let query = format!("WITH RECURSIVE recursive_call AS (
                SELECT * FROM call WHERE id IN ({})
                UNION ALL
                SELECT DISTINCT ON (call.id) call.*
                FROM call JOIN recursive_call ON recursive_call.parent_id = call.id
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
            FROM recursive_call
            ORDER BY block_id", ids);
        let calls = sqlx::query_as::<_, Call>(&query)
            .fetch_all(&self.pool)
            .observe_duration("call")
            .await?;
        Ok(calls)
    }

    async fn load_events(&self) -> Result<Vec<Event>, Error> {
        if self.event_selections.is_empty() {
            return Ok(Vec::new());
        }
        let wildcard = self.event_selections
            .iter()
            .any(|selection| selection.name == "*");
        let names = self.event_selections
            .iter()
            .map(|selection| selection.name.clone())
            .collect::<Vec<String>>();

        let from_block = format!("{:010}", self.from_block);
        let to_block = self.to_block.map(|to_block| format!("{:010}", to_block + 1));

        let build_args = |_last_id: Option<String>, len: usize, limit: i64| {
            let mut args = PgArguments::default();
            let mut sql = String::from("SELECT block_id
                FROM event
                WHERE block_id > $1");
            let mut args_len = 1;
            args.add(&from_block);
            if let Some(to_block) = &to_block {
                args_len += 1;
                args.add(to_block);
                sql.push_str(&format!(" AND block_id < ${}", args_len));
            }
            if !wildcard {
                args_len += 1;
                args.add(&names);
                sql.push_str(&format!(" AND name = ANY(${})", args_len));
            }
            sql.push_str(" ORDER BY block_id");
            args_len += 1;
            args.add(len as i64);
            sql.push_str(&format!(" OFFSET ${}", args_len));
            args_len += 1;
            args.add(limit);
            sql.push_str(&format!(" LIMIT ${}", args_len));

            (sql, args)
        };
        let chunk_limit = 5000;
        let ids = self.load_ids(build_args, "event", chunk_limit).await?;

        let mut query = String::from("SELECT
                id,
                block_id,
                index_in_block::int8,
                phase,
                extrinsic_id,
                call_id,
                name,
                args,
                pos::int8
            FROM event
            WHERE block_id = ANY($1::char(16)[])");
        let mut args = PgArguments::default();
        args.add(&ids);
        if !wildcard {
            query.push_str(&format!(" AND name = ANY($2)"));
            args.add(&names)
        }
        let events = sqlx::query_as_with::<_, Event, _>(&query, args)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_ids(
        &self,
        build_args: impl Fn(Option<String>, usize, i64) -> (String, PgArguments),
        db_table: &'static str,
        limit: i64,
    ) -> Result<Vec<String>, Error> {
        let mut ids = Vec::new();
        let mut blocks = HashSet::new();
        let mut last_id = None;

        'outer: loop {
            let (sql, args) = build_args(last_id, ids.len(), limit);
            let result = sqlx::query_scalar_with::<_, String, _>(&sql, args)
                .fetch_all(&self.pool)
                .observe_duration(db_table)
                .await?;
            if result.is_empty() {
                break
            } else {
                for id in result {
                    let block_id = id.split('-').next().unwrap().to_string();
                    if blocks.len() == self.limit as usize && !blocks.contains(&block_id) {
                        break 'outer;
                    }
                    blocks.insert(block_id);
                    ids.push(id);
                }
                last_id = ids.last().cloned();
            }
        }
        Ok(ids)
    }

    async fn load_messages_enqueued(&self) -> Result<Vec<Event>, Error> {
        if self.gear_message_enqueued_selections.is_empty() {
            return Ok(Vec::new())
        }
        let mut id_gt = format!("{:010}", self.from_block);
        let id_lt = self.to_block.map(|to_block| format!("{:010}", to_block + 1));
        let programs = self.gear_message_enqueued_selections.iter()
            .map(|selection| selection.program.clone())
            .collect::<Vec<String>>();

        let mut ids = Vec::new();
        let mut blocks = HashSet::new();
        let limit: i64 = 2000;

        'outer: loop {
            let mut query = String::from("SELECT event_id
                FROM gear_message_enqueued
                WHERE program = ANY($1) AND event_id > $2");
            let mut args = PgArguments::default();
            args.add(&programs);
            args.add(&id_gt);
            let mut args_len = 2;
            if let Some(id_lt) = &id_lt {
                args_len += 1;
                args.add(id_lt);
                query.push_str(&format!(" AND event_id < ${}", args_len));
            }
            query.push_str(" ORDER BY event_id");
            args_len += 1;
            args.add(limit);
            query.push_str(&format!(" LIMIT ${}", args_len));

            let result = sqlx::query_scalar_with::<_, String, _>(&query, args)
                .fetch_all(&self.pool)
                .observe_duration("gear_message_enqueued")
                .await?;
            if result.is_empty() {
                break
            } else {
                for id in result {
                    let block_id = id.split('-').next().unwrap().to_string();
                    if blocks.len() == self.limit as usize && !blocks.contains(&block_id) {
                        break 'outer;
                    }
                    blocks.insert(block_id);
                    ids.push(id);
                }

                id_gt = ids.last().unwrap().clone();
            }
        }

        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_messages_sent(&self) -> Result<Vec<Event>, Error> {
        if self.gear_user_message_sent_selections.is_empty() {
            return Ok(Vec::new())
        }
        let mut id_gt = format!("{:010}", self.from_block);
        let id_lt = self.to_block.map(|to_block| format!("{:010}", to_block + 1));
        let programs = self.gear_user_message_sent_selections.iter()
            .map(|selection| selection.program.clone())
            .collect::<Vec<String>>();

        let mut ids = Vec::new();
        let mut blocks = HashSet::new();
        let limit: i64 = 2000;

        'outer: loop {
            let mut query = String::from("SELECT event_id
                FROM gear_user_message_sent
                WHERE program = ANY($1) AND event_id > $2");
            let mut args = PgArguments::default();
            args.add(&programs);
            args.add(&id_gt);
            let mut args_len = 2;
            if let Some(id_lt) = &id_lt {
                args_len += 1;
                args.add(id_lt);
                query.push_str(&format!(" AND event_id < ${}", args_len));
            }
            query.push_str(" ORDER BY event_id");
            args_len += 1;
            args.add(limit);
            query.push_str(&format!(" LIMIT ${}", args_len));

            let result = sqlx::query_scalar_with::<_, String, _>(&query, args)
                .fetch_all(&self.pool)
                .observe_duration("gear_user_message_sent")
                .await?;
            if result.is_empty() {
                break
            } else {
                for id in result {
                    let block_id = id.split('-').next().unwrap().to_string();
                    if blocks.len() == self.limit as usize && !blocks.contains(&block_id) {
                        break 'outer;
                    }
                    blocks.insert(block_id);
                    ids.push(id);
                }

                id_gt = ids.last().unwrap().clone();
            }
        }

        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_evm_executed(&self) -> Result<Vec<Event>, Error> {
        if self.evm_executed_selections.is_empty() {
            return Ok(Vec::new())
        }
        let mut ids = Vec::new();
        for selection in self.evm_executed_selections {
            let mut log_ids: Vec<String> = Vec::new();
            let mut blocks = HashSet::new();
            let limit: i64 = 2000;

            let mut id_gt = format!("{:010}", self.from_block);
            let id_lt = self.to_block.map(|to_block| format!("{:010}", to_block + 1));

            'outer: loop {
                let mut query = String::from("SELECT id
                    FROM evm_log
                    WHERE contract = $1 AND id > $2");
                let mut args = PgArguments::default();
                args.add(&selection.contract);
                args.add(&id_gt);
                let mut args_len = 2;
                if let Some(id_lt) = &id_lt {
                    args_len += 1;
                    args.add(id_lt);
                    query.push_str(&format!(" AND id < ${}", args_len));
                }
                for index in 0..=3 {
                    if let Some(topics) = selection.filter.get(index) {
                        if !topics.is_empty() {
                            args_len += 1;
                            args.add(topics);
                            query.push_str(&format!(" AND topic{} = ANY(${})", index, args_len));
                        }
                    }
                }
                args_len += 1;
                args.add(limit);
                query.push_str(&format!(" LIMIT ${}", args_len));

                let result = sqlx::query_scalar_with::<_, String, _>(&query, args)
                    .fetch_all(&self.pool)
                    .observe_duration("evm_log")
                    .await?;
                if result.is_empty() {
                    break
                } else {
                    for id in result {
                        let block_id = id.split('-').next().unwrap().to_string();
                        if blocks.len() == self.limit as usize && !blocks.contains(&block_id) {
                            break 'outer;
                        }
                        blocks.insert(block_id);
                        log_ids.push(id);
                    }

                    id_gt = log_ids.last().unwrap().clone();
                }
            }

            let query = "SELECT event_id
                FROM evm_log
                WHERE id = ANY($1)";
            let mut selection_ids = sqlx::query_scalar::<_, String>(query)
                .bind(&log_ids)
                .fetch_all(&self.pool)
                .await?;
            ids.append(&mut selection_ids);
        }
        ids.dedup();
        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_contracts_events(&self) -> Result<Vec<Event>, Error> {
        if self.contracts_event_selections.is_empty() {
            return Ok(Vec::new())
        }
        let mut id_gt = format!("{:010}", self.from_block);
        let id_lt = self.to_block.map(|to_block| format!("{:010}", to_block + 1));
        let contracts = self.contracts_event_selections.iter()
            .map(|selection| selection.contract.clone())
            .collect::<Vec<String>>();

        let mut ids = Vec::new();
        let mut blocks = HashSet::new();
        let limit: i64 = 2000;

        'outer: loop {
            let mut query = String::from("SELECT event_id
                FROM contracts_contract_emitted
                WHERE contract = ANY($1) AND event_id > $2");
            let mut args = PgArguments::default();
            args.add(&contracts);
            args.add(&id_gt);
            let mut args_len = 2;
            if let Some(id_lt) = &id_lt {
                args_len += 1;
                args.add(id_lt);
                query.push_str(&format!(" AND event_id < ${}", args_len));
            }
            query.push_str(" ORDER BY event_id");
            args_len += 1;
            args.add(limit);
            query.push_str(&format!(" LIMIT ${}", args_len));

            let result = sqlx::query_scalar_with::<_, String, _>(&query, args)
                .fetch_all(&self.pool)
                .observe_duration("contracts_contract_emitted")
                .await?;
            if result.is_empty() {
                break
            } else {
                for id in result {
                    let block_id = id.split('-').next().unwrap().to_string();
                    if blocks.len() == self.limit as usize && !blocks.contains(&block_id) {
                        break 'outer;
                    }
                    blocks.insert(block_id);
                    ids.push(id);
                }

                id_gt = ids.last().unwrap().clone();
            }
        }

        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_evm_logs(&self) -> Result<Vec<EvmLog>, Error> {
        if self.evm_log_selections.is_empty() {
            return Ok(Vec::new())
        }
        let mut ids = Vec::new();
        for selection in self.evm_log_selections {
            let mut log_ids: Vec<String> = Vec::new();
            let mut blocks = HashSet::new();
            let limit: i64 = 2000;

            let mut id_gt = format!("{:010}", self.from_block);
            let id_lt = self.to_block.map(|to_block| format!("{:010}", to_block + 1));

            'outer: loop {
                let mut query = String::from("SELECT event_id
                    FROM frontier_evm_log
                    WHERE event_id > $1");
                let mut args = PgArguments::default();
                args.add(&id_gt);
                let mut args_len = 1;
                if selection.contract != "*" {
                    args_len += 1;
                    args.add(&selection.contract);
                    query.push_str(&format!(" AND contract = ${}", args_len));
                }
                if let Some(id_lt) = &id_lt {
                    args_len += 1;
                    args.add(id_lt);
                    query.push_str(&format!(" AND event_id < ${}", args_len));
                }
                for index in 0..=3 {
                    if let Some(topics) = selection.filter.get(index) {
                        if !topics.is_empty() {
                            args_len += 1;
                            args.add(topics);
                            query.push_str(&format!(" AND topic{} = ANY(${}::char(66)[])", index, args_len));
                        }
                    }
                }
                query.push_str(" ORDER BY event_id");
                args_len += 1;
                args.add(limit);
                query.push_str(&format!(" LIMIT ${}", args_len));

                let result = sqlx::query_scalar_with::<_, String, _>(&query, args)
                    .fetch_all(&self.pool)
                    .observe_duration("frontier_evm_log")
                    .await?;
                if result.is_empty() {
                    break
                } else {
                    for id in result {
                        let block_id = id.split('-').next().unwrap().to_string();
                        if blocks.len() == self.limit as usize && !blocks.contains(&block_id) {
                            break 'outer;
                        }
                        blocks.insert(block_id);
                        log_ids.push(id);
                    }

                    id_gt = log_ids.last().unwrap().clone();
                }
            }
            ids.append(&mut log_ids);
        }
        self.trim_ids(&mut ids);
        let query = "SELECT
                event.id,
                event.block_id,
                event.index_in_block::int8,
                event.phase,
                event.extrinsic_id,
                event.call_id,
                event.name,
                event.args,
                event.pos::int8,
                jsonb_extract_path_text(executed_event.args, '2') AS evm_tx_hash
            FROM event
            JOIN event executed_event
                ON event.extrinsic_id = executed_event.extrinsic_id
                    AND executed_event.name = 'Ethereum.Executed'
            WHERE event.id = ANY($1::char(23)[])";
        let logs = sqlx::query_as::<_, EvmLog>(query)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(logs)
    }

    async fn load_eth_transactions(&self) -> Result<Vec<Call>, Error> {
        if self.eth_transact_selections.is_empty() {
            return Ok(Vec::new())
        }
        let mut ids = Vec::new();
        for selection in self.eth_transact_selections {
            let mut selection_ids: Vec<String> = Vec::new();
            let mut blocks = HashSet::new();
            let limit: i64 = 2000;

            let mut id_gt = format!("{:010}", self.from_block);
            let id_lt = self.to_block.map(|to_block| format!("{:010}", to_block + 1));

            'outer: loop {
                let mut query = String::from("SELECT call_id
                    FROM frontier_ethereum_transaction
                    WHERE call_id > $1");
                let mut args = PgArguments::default();
                args.add(&id_gt);
                let mut args_len = 1;
                if selection.contract != "*" {
                    args_len += 1;
                    args.add(&selection.contract);
                    query.push_str(&format!(" AND contract = ${}", args_len));
                }
                if let Some(id_lt) = &id_lt {
                    args_len += 1;
                    args.add(id_lt);
                    query.push_str(&format!(" AND call_id < ${}", args_len));
                }
                if let Some(sighash) = &selection.sighash {
                    args_len += 1;
                    args.add(sighash);
                    query.push_str(&format!(" AND sighash = ${}", args_len));
                }
                query.push_str(" ORDER BY call_id");
                args_len += 1;
                args.add(limit);
                query.push_str(&format!(" LIMIT ${}", args_len));

                let result = sqlx::query_scalar_with::<_, String, _>(&query, args)
                    .fetch_all(&self.pool)
                    .observe_duration("frontier_ethereum_transaction")
                    .await?;
                if result.is_empty() {
                    break
                } else {
                    for id in result {
                        let block_id = id.split('-').next().unwrap().to_string();
                        if blocks.len() == self.limit as usize && !blocks.contains(&block_id) {
                            break 'outer;
                        }
                        blocks.insert(block_id);
                        selection_ids.push(id);
                    }

                    id_gt = selection_ids.last().unwrap().clone();
                }
            }
            ids.append(&mut selection_ids);
        }
        self.trim_ids(&mut ids);
        let query = "SELECT
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
            FROM call WHERE id = ANY($1::varchar(30)[])";
        let mut calls = sqlx::query_as::<_, Call>(query)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("call")
            .await?;

        let mut parents_ids: Vec<String> = calls.iter()
            .filter_map(|call| call.parent_id.clone())
            .collect();
        parents_ids.sort();
        parents_ids.dedup();
        while !parents_ids.is_empty() {
            let to_load: Vec<String> = parents_ids.iter()
                .filter_map(|parent_id| {
                    let loaded = calls.iter().any(|call| &call.id == parent_id);
                    if loaded {
                        None
                    } else {
                        Some(parent_id.clone())
                    }
                })
                .collect();
            if !to_load.is_empty() {
                let mut parents = sqlx::query_as::<_, Call>(query)
                    .bind(to_load)
                    .fetch_all(&self.pool)
                    .observe_duration("call")
                    .await?;
                calls.append(&mut parents);
            }
            parents_ids = parents_ids.iter()
                .filter_map(|parent_id| {
                    let call = calls.iter().find(|call| &call.id == parent_id).unwrap();
                    call.parent_id.clone()
                })
                .collect();
            parents_ids.sort();
            parents_ids.dedup();
        }
        Ok(calls)
    }

    fn get_block_ids(
        &self,
        calls: &Vec<Call>,
        events: &Vec<Event>,
        evm_logs: &Vec<EvmLog>,
        eth_transactions: &Vec<Call>,
        contracts_events: &Vec<Event>,
        messages_enqueued: &Vec<Event>,
        messages_sent: &Vec<Event>,
        evm_executed: &Vec<Event>,
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
        for transaction in eth_transactions {
            block_ids.push(transaction.block_id.clone());
        }
        for event in contracts_events {
            block_ids.push(event.block_id.clone());
        }
        for event in messages_enqueued {
            block_ids.push(event.block_id.clone());
        }
        for event in messages_sent {
            block_ids.push(event.block_id.clone());
        }
        for event in evm_executed {
            block_ids.push(event.block_id.clone());
        }
        block_ids.sort();
        block_ids.dedup();
        block_ids.truncate(self.limit as usize);
        block_ids
    }

    async fn get_blocks_by_ids(&self, ids: &Vec<String>) -> Result<Vec<BlockHeader>, Error> {
        let query = "SELECT
                id,
                height::int8,
                hash,
                parent_hash,
                state_root,
                extrinsics_root,
                timestamp,
                spec_id,
                validator
            FROM block
            WHERE id = ANY($1::char(16)[])";
        let blocks = sqlx::query_as::<_, BlockHeader>(query)
            .bind(ids)
            .fetch_all(&self.pool)
            .observe_duration("block")
            .await?;
        Ok(blocks)
    }

    async fn get_blocks(&self) -> Result<Vec<BlockHeader>, Error> {
        let query = "SELECT
                id,
                height::int8,
                hash,
                parent_hash,
                state_root,
                extrinsics_root,
                timestamp,
                spec_id,
                validator
            FROM block
            WHERE height >= $1 AND ($2 IS null OR height <= $2)
            ORDER BY height
            LIMIT $3";
        let blocks = sqlx::query_as::<_, BlockHeader>(&query)
            .bind(self.from_block)
            .bind(self.to_block)
            .bind(self.limit)
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
            FROM extrinsic WHERE id = ANY($1::char(23)[])";
        let extrinsics = sqlx::query_as::<_, Extrinsic>(query)
            .bind(ids)
            .fetch_all(&self.pool)
            .observe_duration("extrinsic")
            .await?;
        Ok(extrinsics)
    }

    fn create_batch(
        &self,
        blocks: Vec<BlockHeader>,
        mut events_by_block: HashMap<String, Vec<serde_json::Value>>,
        mut calls_by_block: HashMap<String, Vec<serde_json::Value>>,
        mut extrinsics_by_block: HashMap<String, Vec<serde_json::Value>>,
        logs_by_block: HashMap<String, Vec<serde_json::Value>>,
    ) -> Vec<Batch> {
        for (block_id, mut data) in logs_by_block.into_iter() {
            events_by_block.entry(block_id)
                .or_insert_with(Vec::new)
                .append(&mut data);
        }
        blocks.into_iter()
            .map(|block| {
                let events = events_by_block.remove(&block.id).unwrap_or_default();
                let event_fields = vec!["id", "block_id", "index_in_block", "phase", "evm_tx_hash",
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
        call: &Call,
        data: &CallDataSelection,
        calls: &Vec<Call>,
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

    fn trim_ids(&self, ids: &mut Vec<String>) {
        ids.sort();
        ids.dedup();
        let mut blocks = HashSet::new();
        ids.retain(|id| {
            let block_id = id.split('-').next().unwrap().to_string();
            if blocks.len() == self.limit as usize && !blocks.contains(&block_id) {
                false
            } else {
                blocks.insert(block_id);
                true
            }
        });
    }
}

impl BatchOptions {
    pub(in super) fn loader(&self, pool: Pool<Postgres>) -> BatchLoader {
        BatchLoader {
            pool,
            limit: self.limit,
            from_block: self.from_block,
            to_block: self.to_block,
            include_all_blocks: self.include_all_blocks,
            call_selections: &self.call_selections,
            event_selections: &self.event_selections,
            evm_log_selections: &self.evm_log_selections,
            eth_transact_selections: &self.eth_transact_selections,
            contracts_event_selections: &self.contracts_event_selections,
            gear_message_enqueued_selections: &self.gear_message_enqueued_selections,
            gear_user_message_sent_selections: &self.gear_user_message_sent_selections,
            evm_executed_selections: &self.evm_executed_selections,
        }
    }
}
