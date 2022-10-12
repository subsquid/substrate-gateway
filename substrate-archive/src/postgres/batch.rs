use super::serializer::{CallSerializer, EventSerializer, EvmLogSerializer, ExtrinsicSerializer};
use super::utils::unify_and_merge;
use super::{BatchOptions, DatabaseType};
use crate::entities::{Batch, BlockHeader, Call, Event, EvmLog, Extrinsic};
use crate::error::Error;
use crate::fields::{CallFields, EventFields, EvmLogFields, ExtrinsicFields};
use crate::metrics::ObserverExt;
use crate::selection::{
    AcalaEvmEventSelection, AcalaEvmLog, CallDataSelection, CallSelection, ContractsEventSelection,
    EthTransactSelection, EventDataSelection, EventSelection, EvmLogSelection,
    GearMessageEnqueuedSelection, GearUserMessageSentSelection,
};
use sqlx::postgres::{PgArguments, Postgres};
use sqlx::{Arguments, Pool};
use std::cmp::min;
use std::collections::{HashMap, HashSet};

pub struct BatchLoader<'a> {
    pool: Pool<Postgres>,
    database_type: DatabaseType,
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
    acala_evm_executed_selections: &'a Vec<AcalaEvmEventSelection>,
    acala_evm_executed_failed_selections: &'a Vec<AcalaEvmEventSelection>,
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
        let mut acala_evm_executed = self
            .load_acala_evm_event(
                self.acala_evm_executed_selections,
                "acala_evm_executed",
                "acala_evm_executed_log",
            )
            .await?;
        let mut acala_evm_failed = self
            .load_acala_evm_event(
                self.acala_evm_executed_failed_selections,
                "acala_evm_executed_failed",
                "acala_evm_executed_failed_log",
            )
            .await?;
        let blocks = if self.include_all_blocks {
            self.load_blocks().await?
        } else {
            let mut ids = vec![];
            calls
                .iter()
                .for_each(|call| ids.push(call.block_id.clone()));
            events
                .iter()
                .for_each(|event| ids.push(event.block_id.clone()));
            evm_logs
                .iter()
                .for_each(|log| ids.push(log.block_id.clone()));
            eth_transactions
                .iter()
                .for_each(|call| ids.push(call.block_id.clone()));
            contracts_events
                .iter()
                .for_each(|event| ids.push(event.block_id.clone()));
            messages_enqueued
                .iter()
                .for_each(|event| ids.push(event.block_id.clone()));
            messages_sent
                .iter()
                .for_each(|event| ids.push(event.block_id.clone()));
            acala_evm_executed
                .iter()
                .for_each(|event| ids.push(event.block_id.clone()));
            acala_evm_failed
                .iter()
                .for_each(|event| ids.push(event.block_id.clone()));
            ids.sort();
            ids.dedup();
            ids.truncate(self.limit as usize);

            calls.retain(|call| ids.contains(&call.block_id));
            events.retain(|event| ids.contains(&event.block_id));
            evm_logs.retain(|evm_log| ids.contains(&evm_log.block_id));
            eth_transactions.retain(|call| ids.contains(&call.block_id));
            contracts_events.retain(|event| ids.contains(&event.block_id));
            messages_enqueued.retain(|event| ids.contains(&event.block_id));
            messages_sent.retain(|event| ids.contains(&event.block_id));
            acala_evm_executed.retain(|event| ids.contains(&event.block_id));
            acala_evm_failed.retain(|event| ids.contains(&event.block_id));
            self.load_blocks_by_ids(&ids).await?
        };

        let mut extrinsic_fields: HashMap<String, ExtrinsicFields> = HashMap::new();
        let mut event_fields: HashMap<String, EventFields> = HashMap::new();
        let mut log_fields: HashMap<String, EvmLogFields> = HashMap::new();
        let mut call_fields: HashMap<String, CallDataSelection> = HashMap::new();

        let mut call_fields_to_load: HashMap<String, CallFields> = HashMap::new();

        let mut call_lookup: HashMap<String, &Call> = HashMap::new();
        for call in &eth_transactions {
            call_lookup.insert(call.id.clone(), call);
        }
        for call in &calls {
            call_lookup.insert(call.id.clone(), call);
        }

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
                                selection.data.extrinsic.clone(),
                            );
                        }
                    }
                    self.visit_parent_call(&call, &selection.data, &call_lookup, &mut call_fields);
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
                                selection.data.extrinsic.clone(),
                            );
                        }
                    }
                    self.visit_parent_call(&call, &selection.data, &call_lookup, &mut call_fields);
                }
            }
        }

        let mut process_event = |event: &Event, data: &EventDataSelection| {
            event_fields
                .entry(event.id.clone())
                .and_modify(|fields| fields.merge(&data.event))
                .or_insert_with(|| data.event.clone());

            if let Some(extrinsic_id) = &event.extrinsic_id {
                if data.event.extrinsic.any() {
                    extrinsic_fields
                        .entry(extrinsic_id.clone())
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
                            call_fields_to_load.insert(call_id.clone(), data.event.call.clone());
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
        for event in &acala_evm_executed {
            for selection in self.acala_evm_executed_selections {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut acala_evm_executed);
        for event in &acala_evm_failed {
            for selection in self.acala_evm_executed_failed_selections {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut acala_evm_failed);
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
                    log_fields
                        .entry(log.id.clone())
                        .and_modify(|fields| fields.merge(&selection.data.event))
                        .or_insert_with(|| selection.data.event.clone());

                    if let Some(extrinsic_id) = &log.extrinsic_id {
                        if selection.data.event.extrinsic.any() {
                            extrinsic_fields
                                .entry(extrinsic_id.to_string())
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
                                call_fields_to_load
                                    .insert(call_id.to_string(), selection.data.event.call.clone());
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
                        call_fields_to_load.insert(extrinsic.call_id.clone(), fields.call.clone());
                    }
                }
            }
        }

        if !call_fields_to_load.is_empty() {
            let call_ids = call_fields_to_load.keys().cloned().collect();
            let mut additional_calls = self.load_calls_by_ids(&call_ids).await?;
            let mut call_lookup: HashMap<String, &Call> = HashMap::new();
            for call in &additional_calls {
                call_lookup.insert(call.id.clone(), call);
            }
            for call in &additional_calls {
                if let Some(fields) = call_fields_to_load.remove(&call.id) {
                    let data_selection = CallDataSelection {
                        call: fields,
                        extrinsic: ExtrinsicFields::new(false),
                    };
                    self.visit_parent_call(call, &data_selection, &call_lookup, &mut call_fields);
                    call_fields.insert(call.id.clone(), data_selection);
                }
            }
            calls.append(&mut additional_calls);
        }

        let mut events_by_block: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for event in events {
            let fields = event_fields.get(&event.id).unwrap();
            let serializer = EventSerializer {
                event: &event,
                fields,
            };
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
            if let Some(fields) = call_fields.remove(&call.id) {
                let serializer = CallSerializer {
                    call: &call,
                    fields: &fields,
                };
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
            let serializer = ExtrinsicSerializer {
                extrinsic: &extrinsic,
                fields: extrinsic_fields.get(&extrinsic.id).unwrap(),
            };
            let data = serde_json::to_value(serializer).unwrap();
            if extrinsics_by_block.contains_key(&extrinsic.block_id) {
                extrinsics_by_block
                    .get_mut(&extrinsic.block_id)
                    .unwrap()
                    .push(data);
            } else {
                extrinsics_by_block.insert(extrinsic.block_id.clone(), vec![data]);
            }
        }
        let batch = self.create_batch(
            blocks,
            events_by_block,
            calls_by_block,
            extrinsics_by_block,
            logs_by_block,
        );
        Ok(batch)
    }

    async fn load_calls(&self) -> Result<Vec<Call>, Error> {
        if self.call_selections.is_empty() {
            return Ok(Vec::new());
        }
        let wildcard = self
            .call_selections
            .iter()
            .any(|selection| selection.name == "*");
        let names = self
            .call_selections
            .iter()
            .map(|selection| selection.name.clone())
            .collect::<Vec<String>>();

        let from_block = format!("{:010}", self.from_block);
        let to_block = self
            .to_block
            .map(|to_block| format!("{:010}", to_block + 1));

        let build_args = |_last_id: Option<String>, len: usize, limit: i64| {
            let mut args = PgArguments::default();
            let mut sql = String::from(
                "SELECT block_id
                FROM call
                WHERE block_id > $1",
            );
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

        let mut query = String::from(
            "SELECT
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
            FROM call WHERE block_id = ANY($1::char(16)[])",
        );
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
        let mut parents_ids: Vec<String> = calls
            .iter()
            .filter_map(|call| call.parent_id.clone())
            .collect();
        parents_ids.sort();
        parents_ids.dedup();
        while !parents_ids.is_empty() {
            let to_load: Vec<String> = parents_ids
                .iter()
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
            parents_ids = parents_ids
                .iter()
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
        let ids = ids
            .iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<String>>()
            .join(", ");
        let query = format!(
            "WITH RECURSIVE recursive_call AS (
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
            ORDER BY block_id",
            ids
        );
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
        let wildcard = self
            .event_selections
            .iter()
            .any(|selection| selection.name == "*");
        let names = self
            .event_selections
            .iter()
            .map(|selection| selection.name.clone())
            .collect::<Vec<String>>();

        let mut ids = vec![];
        let mut range_width = self.limit;
        let mut total_range = 0;
        let mut from_block = self.from_block;
        let head = if let Some(to_block) = self.to_block {
            to_block
        } else {
            let query = "SELECT height::int4 as head FROM block ORDER BY height DESC LIMIT 1";
            let head = sqlx::query_scalar::<_, i32>(query)
                .fetch_optional(&self.pool)
                .observe_duration("block")
                .await?;
            if let Some(head) = head {
                head
            } else {
                return Ok(vec![]);
            }
        };

        'outer: loop {
            let block_gt = format!("{:010}", from_block);
            let to_block = min(from_block + range_width, head);
            let block_lt = format!("{:010}", to_block + 1);

            let table = match self.database_type {
                DatabaseType::Cockroach => "event@idx_event__name__block",
                DatabaseType::Postgres => "event",
            };
            let mut args = PgArguments::default();
            let mut sql = format!(
                "SELECT block_id
                FROM {}
                WHERE block_id > $1 AND block_id < $2",
                table
            );
            let mut args_len = 2;
            args.add(&block_gt);
            args.add(&block_lt);
            if !wildcard {
                args_len += 1;
                args.add(&names);
                sql.push_str(&format!(" AND name = ANY(${})", args_len));
            }
            sql.push_str(" ORDER BY block_id");

            let mut blocks = sqlx::query_scalar_with::<_, String, _>(&sql, args)
                .fetch_all(&self.pool)
                .observe_duration("event")
                .await?;
            blocks.dedup();
            let len = i32::try_from(blocks.len()).unwrap();
            total_range += range_width;

            for block_id in blocks {
                ids.push(block_id);
                if ids.len() == self.limit as usize {
                    break 'outer;
                }
            }

            if to_block == head {
                break;
            }

            range_width = if len == 0 {
                min(range_width * 10, 100_000)
            } else {
                let total_blocks = i32::try_from(ids.len()).unwrap();
                min((total_range / total_blocks) * (self.limit - len), 100_000)
            };
            from_block = to_block + 1;
        }

        let mut query = String::from(
            "SELECT
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
            WHERE block_id = ANY($1::char(16)[])",
        );
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
                break;
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
            return Ok(Vec::new());
        }
        let id_gt = format!("{:010}", self.from_block);
        let id_lt = self
            .to_block
            .map(|to_block| format!("{:010}", to_block + 1));
        let programs = self
            .gear_message_enqueued_selections
            .iter()
            .map(|selection| selection.program.clone())
            .collect::<Vec<String>>();

        let build_args = |last_id: Option<String>, _len: usize, limit: i64| {
            let mut args = PgArguments::default();
            let mut sql = String::from(
                "SELECT event_id
                FROM gear_message_enqueued
                WHERE program = ANY($1) AND event_id > $2",
            );
            let mut args_len = 2;
            args.add(&programs);
            if let Some(last_id) = &last_id {
                args.add(last_id);
            } else {
                args.add(&id_gt);
            }
            if let Some(id_lt) = &id_lt {
                args_len += 1;
                args.add(id_lt);
                sql.push_str(&format!(" AND event_id < ${}", args_len));
            }
            sql.push_str(" ORDER BY event_id");
            args_len += 1;
            args.add(limit);
            sql.push_str(&format!(" LIMIT ${}", args_len));

            (sql, args)
        };
        let chunk_limit = 2000;
        let ids = self
            .load_ids(build_args, "gear_message_enqueued", chunk_limit)
            .await?;

        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_messages_sent(&self) -> Result<Vec<Event>, Error> {
        if self.gear_user_message_sent_selections.is_empty() {
            return Ok(Vec::new());
        }
        let id_gt = format!("{:010}", self.from_block);
        let id_lt = self
            .to_block
            .map(|to_block| format!("{:010}", to_block + 1));
        let programs = self
            .gear_user_message_sent_selections
            .iter()
            .map(|selection| selection.program.clone())
            .collect::<Vec<String>>();

        let build_args = |last_id: Option<String>, _len: usize, limit: i64| {
            let mut args = PgArguments::default();
            let mut sql = String::from(
                "SELECT event_id
                FROM gear_user_message_sent
                WHERE program = ANY($1) AND event_id > $2",
            );
            let mut args_len = 2;
            args.add(&programs);
            if let Some(last_id) = &last_id {
                args.add(last_id);
            } else {
                args.add(&id_gt);
            }
            if let Some(id_lt) = &id_lt {
                args_len += 1;
                args.add(id_lt);
                sql.push_str(&format!(" AND event_id < ${}", args_len));
            }
            sql.push_str(" ORDER BY event_id");
            args_len += 1;
            args.add(limit);
            sql.push_str(&format!(" LIMIT ${}", args_len));

            (sql, args)
        };
        let chunk_limit = 2000;
        let ids = self
            .load_ids(build_args, "gear_user_message_sent", chunk_limit)
            .await?;

        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    fn is_acala_evm_logs_empty(&self, logs: &Vec<AcalaEvmLog>) -> bool {
        for log in logs {
            if log.contract.is_some() {
                return false;
            }
            for topics in &log.filter {
                if !topics.is_empty() {
                    return false;
                }
            }
        }
        true
    }

    async fn query_acala_evm_event(
        &self,
        selection: &AcalaEvmEventSelection,
        event_table: &'static str,
    ) -> Result<Vec<String>, Error> {
        let id_gt = format!("{:010}", self.from_block);
        let id_lt = self
            .to_block
            .map(|to_block| format!("{:010}", to_block + 1));

        let build_args = |last_id: Option<String>, _len: usize, limit: i64| {
            let mut args = PgArguments::default();
            let mut sql = format!(
                "SELECT event_id
                FROM {}
                WHERE event_id > $1",
                event_table
            );
            let mut args_len = 1;
            if let Some(last_id) = &last_id {
                args.add(last_id);
            } else {
                args.add(&id_gt);
            }
            if let Some(id_lt) = &id_lt {
                args_len += 1;
                args.add(id_lt);
                sql.push_str(&format!(" AND event_id < ${}", args_len));
            }
            if selection.contract != "*" {
                args_len += 1;
                args.add(&selection.contract);
                sql.push_str(&format!(" AND contract = ${}", args_len));
            }
            sql.push_str(" ORDER BY event_id");
            args_len += 1;
            args.add(limit);
            sql.push_str(&format!(" LIMIT ${}", args_len));

            (sql, args)
        };
        let chunk_limit = 2000;
        self.load_ids(build_args, event_table, chunk_limit).await
    }

    async fn query_acala_evm_event_log(
        &self,
        selection: &AcalaEvmEventSelection,
        log_table: &'static str,
    ) -> Result<Vec<String>, Error> {
        let mut ids: Vec<String> = Vec::new();

        for log in &selection.logs {
            let id_gt = format!("{:010}", self.from_block);
            let id_lt = self
                .to_block
                .map(|to_block| format!("{:010}", to_block + 1));

            let build_args = |last_id: Option<String>, _len: usize, limit: i64| {
                let mut args = PgArguments::default();
                let mut sql = format!(
                    "SELECT id
                    FROM {}
                    WHERE id > $1",
                    log_table
                );
                let mut args_len = 1;
                if let Some(last_id) = &last_id {
                    args.add(last_id);
                } else {
                    args.add(&id_gt);
                }
                if let Some(id_lt) = &id_lt {
                    args_len += 1;
                    args.add(id_lt);
                    sql.push_str(&format!(" AND id < ${}", args_len));
                }
                if selection.contract != "*" {
                    args_len += 1;
                    args.add(&selection.contract);
                    sql.push_str(&format!(" AND event_contract = ${}", args_len));
                }
                if let Some(contract) = &log.contract {
                    args_len += 1;
                    args.add(contract);
                    sql.push_str(&format!(" AND contract = ${}", args_len));
                }
                for idx in 0..=3 {
                    if let Some(topics) = log.filter.get(idx) {
                        if !topics.is_empty() {
                            args_len += 1;
                            args.add(topics);
                            sql.push_str(&format!(" AND topic{} = ANY(${})", idx, args_len));
                        }
                    }
                }
                sql.push_str(" ORDER BY id");
                args_len += 1;
                args.add(limit);
                sql.push_str(&format!(" LIMIT ${}", args_len));

                (sql, args)
            };
            let chunk_limit = 2000;
            let mut log_ids = self.load_ids(build_args, log_table, chunk_limit).await?;
            ids.append(&mut log_ids);
        }
        self.trim_ids(&mut ids);

        let query = "SELECT event_id
            FROM acala_evm_executed_log
            WHERE id = ANY($1::char(23)[])";
        let selection_ids = sqlx::query_scalar::<_, String>(query)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("acala_evm_executed_log")
            .await?;
        Ok(selection_ids)
    }

    async fn load_acala_evm_event(
        &self,
        selections: &Vec<AcalaEvmEventSelection>,
        event_table: &'static str,
        log_table: &'static str,
    ) -> Result<Vec<Event>, Error> {
        if selections.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for selection in selections {
            let mut selection_ids = if self.is_acala_evm_logs_empty(&selection.logs) {
                self.query_acala_evm_event(selection, event_table).await?
            } else {
                self.query_acala_evm_event_log(selection, log_table).await?
            };
            ids.append(&mut selection_ids);
        }
        self.trim_ids(&mut ids);
        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_contracts_events(&self) -> Result<Vec<Event>, Error> {
        if self.contracts_event_selections.is_empty() {
            return Ok(Vec::new());
        }
        let id_gt = format!("{:010}", self.from_block);
        let id_lt = self
            .to_block
            .map(|to_block| format!("{:010}", to_block + 1));
        let contracts = self
            .contracts_event_selections
            .iter()
            .map(|selection| selection.contract.clone())
            .collect::<Vec<String>>();

        let build_args = |last_id: Option<String>, _len: usize, limit: i64| {
            let mut args = PgArguments::default();
            let mut sql = String::from(
                "SELECT event_id
                FROM contracts_contract_emitted
                WHERE contract = ANY($1) AND event_id > $2",
            );
            let mut args_len = 2;
            args.add(&contracts);
            if let Some(last_id) = &last_id {
                args.add(last_id);
            } else {
                args.add(&id_gt);
            }
            if let Some(id_lt) = &id_lt {
                args_len += 1;
                args.add(id_lt);
                sql.push_str(&format!(" AND event_id < ${}", args_len));
            }
            sql.push_str(" ORDER BY event_id");
            args_len += 1;
            args.add(limit);
            sql.push_str(&format!(" LIMIT ${}", args_len));

            (sql, args)
        };
        let chunk_limit = 2000;
        let ids = self
            .load_ids(build_args, "contracts_contract_emitted", chunk_limit)
            .await?;

        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    fn group_evm_selections(
        &'a self,
        selections: &'a Vec<EvmLogSelection>,
    ) -> Vec<Vec<&EvmLogSelection>> {
        let mut grouped: Vec<Vec<&EvmLogSelection>> = vec![];
        for selection in selections {
            let group = grouped.iter_mut().find(|group| {
                group.iter().any(|group_selection| {
                    if group_selection.filter != selection.filter {
                        return false;
                    }
                    true
                })
            });
            if let Some(group) = group {
                group.push(selection);
            } else {
                grouped.push(vec![selection]);
            }
        }
        grouped
    }

    async fn load_evm_logs(&self) -> Result<Vec<EvmLog>, Error> {
        if self.evm_log_selections.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for selections in self.group_evm_selections(&self.evm_log_selections) {
            let id_gt = format!("{:010}", self.from_block);
            let id_lt = self
                .to_block
                .map(|to_block| format!("{:010}", to_block + 1));

            let build_args = |last_id: Option<String>, _len: usize, limit: i64| {
                let mut args = PgArguments::default();
                let mut sql = String::from(
                    "SELECT event_id
                    FROM frontier_evm_log
                    WHERE event_id > $1",
                );
                let mut args_len = 1;
                if let Some(last_id) = &last_id {
                    args.add(last_id);
                } else {
                    args.add(&id_gt);
                }
                if let Some(id_lt) = &id_lt {
                    args_len += 1;
                    args.add(id_lt);
                    sql.push_str(&format!(" AND event_id < ${}", args_len));
                }
                let wildcard = selections.iter().any(|selection| selection.contract == "*");
                let contracts: Vec<String> = selections
                    .iter()
                    .map(|selection| selection.contract.clone())
                    .collect();
                if !wildcard {
                    args_len += 1;
                    args.add(&contracts);
                    sql.push_str(&format!(" AND contract = ANY(${}::char(42)[])", args_len));
                }
                for index in 0..=3 {
                    if let Some(topics) = selections[0].filter.get(index) {
                        if !topics.is_empty() {
                            args_len += 1;
                            args.add(topics);
                            sql.push_str(&format!(
                                " AND topic{} = ANY(${}::char(66)[])",
                                index, args_len
                            ));
                        }
                    }
                }
                sql.push_str(" ORDER BY event_id");
                args_len += 1;
                args.add(limit);
                sql.push_str(&format!(" LIMIT ${}", args_len));

                (sql, args)
            };
            let chunk_limit = 2000;
            let mut log_ids = self
                .load_ids(build_args, "frontier_evm_log", chunk_limit)
                .await?;
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
                COALESCE(
                    jsonb_extract_path_text(executed_event.args, '2'),
                    jsonb_extract_path_text(executed_event.args, 'transactionHash')
                ) AS evm_tx_hash
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
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for selection in self.eth_transact_selections {
            let id_gt = format!("{:010}", self.from_block);
            let id_lt = self
                .to_block
                .map(|to_block| format!("{:010}", to_block + 1));

            let build_args = |last_id: Option<String>, _len: usize, limit: i64| {
                let mut args = PgArguments::default();
                let mut sql = String::from(
                    "SELECT call_id
                    FROM frontier_ethereum_transaction
                    WHERE call_id > $1",
                );
                let mut args_len = 1;
                if let Some(last_id) = &last_id {
                    args.add(last_id);
                } else {
                    args.add(&id_gt);
                }
                if let Some(id_lt) = &id_lt {
                    args_len += 1;
                    args.add(id_lt);
                    sql.push_str(&format!(" AND call_id < ${}", args_len));
                }
                if selection.contract != "*" {
                    args_len += 1;
                    args.add(&selection.contract);
                    sql.push_str(&format!(" AND contract = ${}", args_len));
                }
                if let Some(sighash) = &selection.sighash {
                    args_len += 1;
                    args.add(sighash);
                    sql.push_str(&format!(" AND sighash = ${}", args_len));
                }
                sql.push_str(" ORDER BY call_id");
                args_len += 1;
                args.add(limit);
                sql.push_str(&format!(" LIMIT ${}", args_len));

                (sql, args)
            };
            let chunk_limit = 2000;
            let mut selection_ids = self
                .load_ids(build_args, "frontier_ethereum_transaction", chunk_limit)
                .await?;
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

        let mut parents_ids: Vec<String> = calls
            .iter()
            .filter_map(|call| call.parent_id.clone())
            .collect();
        parents_ids.sort();
        parents_ids.dedup();
        while !parents_ids.is_empty() {
            let to_load: Vec<String> = parents_ids
                .iter()
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
            parents_ids = parents_ids
                .iter()
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

    async fn load_blocks_by_ids(&self, ids: &Vec<String>) -> Result<Vec<BlockHeader>, Error> {
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

    async fn load_blocks(&self) -> Result<Vec<BlockHeader>, Error> {
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
            events_by_block
                .entry(block_id)
                .or_insert_with(Vec::new)
                .append(&mut data);
        }
        blocks
            .into_iter()
            .map(|block| {
                let events = events_by_block.remove(&block.id).unwrap_or_default();
                let event_fields = vec![
                    "id",
                    "blockId",
                    "indexInBlock",
                    "phase",
                    "evmTxHash",
                    "extrinsicId",
                    "callId",
                    "name",
                    "args",
                    "pos",
                ];
                let deduplicated_events = unify_and_merge(events, event_fields);
                Batch {
                    extrinsics: extrinsics_by_block.remove(&block.id).unwrap_or_default(),
                    calls: calls_by_block.remove(&block.id).unwrap_or_default(),
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
        call_lookup: &HashMap<String, &Call>,
        call_fields: &mut HashMap<String, CallDataSelection>,
    ) {
        if let Some(parent_id) = &call.parent_id {
            if data.call.parent.any() {
                let parent = call_lookup
                    .get(parent_id)
                    .expect("parent call expected to be loaded");
                let parent_fields = CallDataSelection {
                    call: CallFields::from_parent(&data.call.parent),
                    extrinsic: ExtrinsicFields::new(false),
                };
                self.visit_parent_call(&parent, &parent_fields, call_lookup, call_fields);
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
    pub(super) fn loader(&self, pool: Pool<Postgres>, database_type: DatabaseType) -> BatchLoader {
        BatchLoader {
            pool,
            database_type,
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
            acala_evm_executed_selections: &self.acala_evm_executed_selections,
            acala_evm_executed_failed_selections: &self.acala_evm_executed_failed_selections,
        }
    }
}
