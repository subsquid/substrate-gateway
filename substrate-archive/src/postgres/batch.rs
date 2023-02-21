use super::serializer::{CallSerializer, EventSerializer, EvmLogSerializer, ExtrinsicSerializer};
use super::utils::unify_and_merge;
use super::DatabaseType;
use crate::archive::Selections;
use crate::entities::{Batch, BlockHeader, Call, Event, EvmLog, Extrinsic};
use crate::error::Error;
use crate::fields::{CallFields, EventFields, EvmLogFields, ExtrinsicFields};
use crate::metrics::ObserverExt;
use crate::selection::{
    AcalaEvmEventSelection, AcalaEvmLog, CallDataSelection, CallSelection, ContractsEventSelection,
    EthTransactSelection, EventDataSelection, EventSelection, EvmLogSelection,
    GearMessageEnqueuedSelection, GearUserMessageSentSelection,
};
use crate::sql::{select, Parameters};
use sqlx::postgres::Postgres;
use sqlx::Pool;
use std::collections::HashMap;

#[derive(Clone)]
pub struct BatchLoader {
    pool: Pool<Postgres>,
    database_type: DatabaseType,
}

pub struct BatchResponse {
    pub data: Vec<Batch>,
    pub last_block: i32,
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

impl BatchLoader {
    pub fn new(pool: Pool<Postgres>, database_type: DatabaseType) -> BatchLoader {
        BatchLoader {
            pool,
            database_type,
        }
    }

    pub async fn load(
        &self,
        from_block: i32,
        to_block: i32,
        include_all_blocks: bool,
        selections: &Selections,
    ) -> Result<BatchResponse, Error> {
        let mut calls = self
            .load_calls(from_block, to_block, &selections.call)
            .await?;
        let mut events = self
            .load_events(from_block, to_block, &selections.event)
            .await?;
        let evm_logs = self
            .load_evm_logs(from_block, to_block, &selections.evm_log)
            .await?;
        let (mut eth_transactions, mut eth_executed) = self
            .load_eth_transactions(from_block, to_block, &selections.eth_transact)
            .await?;
        let mut contracts_events = self
            .load_contracts_events(from_block, to_block, &selections.contracts_event)
            .await?;
        let mut messages_enqueued = self
            .load_messages_enqueued(from_block, to_block, &selections.gear_message_enqueued)
            .await?;
        let mut messages_sent = self
            .load_messages_sent(from_block, to_block, &selections.gear_user_message_sent)
            .await?;
        let mut acala_evm_executed = self
            .load_acala_evm_event(
                from_block,
                to_block,
                &selections.acala_evm_executed,
                "acala_evm_executed",
                "acala_evm_executed_log",
            )
            .await?;
        let mut acala_evm_failed = self
            .load_acala_evm_event(
                from_block,
                to_block,
                &selections.acala_evm_executed_failed,
                "acala_evm_executed_failed",
                "acala_evm_executed_failed_log",
            )
            .await?;
        let blocks = if include_all_blocks {
            self.load_blocks(from_block, to_block).await?
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
            for selection in &selections.eth_transact {
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
            for selection in &selections.call {
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
            for selection in &selections.event {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        for event in &messages_enqueued {
            for selection in &selections.gear_message_enqueued {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut messages_enqueued);
        for event in &messages_sent {
            for selection in &selections.gear_user_message_sent {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut messages_sent);
        for event in &acala_evm_executed {
            for selection in &selections.acala_evm_executed {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut acala_evm_executed);
        for event in &acala_evm_failed {
            for selection in &selections.acala_evm_executed_failed {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut acala_evm_failed);
        for event in &contracts_events {
            for selection in &selections.contracts_event {
                if selection.r#match(event) {
                    process_event(event, &selection.data);
                }
            }
        }
        events.append(&mut contracts_events);
        for event in &eth_executed {
            let f = EventFields::new(true);
            event_fields
                .entry(event.id.clone())
                .and_modify(|fields| fields.merge(&f))
                .or_insert_with(|| f);
        }
        events.append(&mut eth_executed);

        for log in &evm_logs {
            for selection in &selections.evm_log {
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
            let call_ids: Vec<String> = call_fields_to_load.keys().cloned().collect();
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
        Ok(BatchResponse {
            data: batch,
            last_block: to_block,
        })
    }

    async fn load_calls(
        &self,
        from_block: i32,
        to_block: i32,
        selections: &Vec<CallSelection>,
    ) -> Result<Vec<Call>, Error> {
        if selections.is_empty() {
            return Ok(vec![]);
        }

        let wildcard = selections.iter().any(|selection| selection.name == "*");
        let names = selections
            .iter()
            .map(|selection| selection.name.clone())
            .collect::<Vec<String>>();
        let from_block = format!("{:010}", from_block);
        let to_block = format!("{:010}", to_block + 1);

        let mut params = Parameters::default();
        let mut query = select([
            "id",
            "parent_id",
            "block_id",
            "extrinsic_id",
            "name",
            "args",
            "success",
            "error",
            "origin",
            "pos::int8",
        ])
        .from("call")
        .where_(format!("block_id > {}", params.add(&from_block)))
        .where_(format!("block_id < {}", params.add(&to_block)));
        if !wildcard {
            query = query.where_(format!("name = ANY({})", params.add(&names)));
        }
        let mut calls = sqlx::query_as_with::<_, Call, _>(&query.to_string(), params.get())
            .fetch_all(&self.pool)
            .observe_duration("call")
            .await?;

        let query = select([
            "id",
            "parent_id",
            "block_id",
            "extrinsic_id",
            "name",
            "args",
            "success",
            "error",
            "origin",
            "pos::int8",
        ])
        .from("call")
        .where_("id = ANY($1::varchar(30)[])")
        .to_string();
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
                let mut parents = sqlx::query_as::<_, Call>(&query)
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

    async fn load_calls_by_ids(&self, ids: &[String]) -> Result<Vec<Call>, Error> {
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

    async fn load_events(
        &self,
        from_block: i32,
        to_block: i32,
        selections: &Vec<EventSelection>,
    ) -> Result<Vec<Event>, Error> {
        if selections.is_empty() {
            return Ok(vec![]);
        }
        let wildcard = selections.iter().any(|selection| selection.name == "*");
        let names = selections
            .iter()
            .map(|selection| format!("'{}'", selection.name))
            .collect::<Vec<String>>()
            .join(", ");

        let from_block = format!("{:010}", from_block);
        let to_block = format!("{:010}", to_block + 1);

        let table = match self.database_type {
            DatabaseType::Cockroach => {
                if wildcard {
                    "event"
                } else {
                    "event@idx_event__name__block"
                }
            }
            DatabaseType::Postgres => "event",
        };

        let mut params = Parameters::default();
        let mut query = select([
            "id",
            "block_id",
            "index_in_block::int8",
            "phase",
            "extrinsic_id",
            "call_id",
            "name",
            "args",
            "pos::int8",
        ])
        .from(table)
        .where_(format!("block_id > {}", params.add(&from_block)))
        .where_(format!("block_id < {}", params.add(&to_block)));
        if !wildcard {
            query = query.where_(format!("name IN ({})", &names));
        }
        let events = sqlx::query_as_with::<_, Event, _>(&query.to_string(), params.get())
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_messages_enqueued(
        &self,
        from_block: i32,
        to_block: i32,
        selections: &Vec<GearMessageEnqueuedSelection>,
    ) -> Result<Vec<Event>, Error> {
        if selections.is_empty() {
            return Ok(Vec::new());
        }

        let programs = selections
            .iter()
            .map(|selection| selection.program.clone())
            .collect::<Vec<String>>();
        let from_block = format!("{:010}", from_block);
        let to_block = format!("{:010}", to_block + 1);

        let query = "SELECT event_id
            FROM gear_message_enqueued
            WHERE program = ANY($1) AND event_id > $2 AND event_id < $3
            ORDER BY event_id";
        let ids = sqlx::query_scalar::<_, String>(&query)
            .bind(&programs)
            .bind(&from_block)
            .bind(&to_block)
            .fetch_all(&self.pool)
            .observe_duration("gear_message_enqueued")
            .await?;

        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_messages_sent(
        &self,
        from_block: i32,
        to_block: i32,
        selections: &Vec<GearUserMessageSentSelection>,
    ) -> Result<Vec<Event>, Error> {
        if selections.is_empty() {
            return Ok(Vec::new());
        }

        let programs = selections
            .iter()
            .map(|selection| selection.program.clone())
            .collect::<Vec<String>>();
        let from_block = format!("{:010}", from_block);
        let to_block = format!("{:010}", to_block + 1);

        let query = "SELECT event_id
            FROM gear_user_message_sent
            WHERE program = ANY($1) AND event_id > $2 AND event_id < $3
            ORDER BY event_id";
        let ids = sqlx::query_scalar::<_, String>(&query)
            .bind(&programs)
            .bind(&from_block)
            .bind(&to_block)
            .fetch_all(&self.pool)
            .observe_duration("gear_user_message_sent")
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
        from_block: i32,
        to_block: i32,
        selection: &AcalaEvmEventSelection,
        event_table: &'static str,
    ) -> Result<Vec<String>, Error> {
        let from_block = format!("{:010}", from_block);
        let to_block = format!("{:010}", to_block + 1);

        let query = format!(
            "SELECT event_id
            FROM {}
            WHERE contract = $1 AND event_id > $2 AND event_id < $3
            ORDER BY event_id",
            event_table
        );
        let ids = sqlx::query_scalar::<_, String>(&query)
            .bind(&selection.contract)
            .bind(&from_block)
            .bind(&to_block)
            .fetch_all(&self.pool)
            .observe_duration(event_table)
            .await?;
        Ok(ids)
    }

    async fn query_acala_evm_event_log(
        &self,
        from_block: i32,
        to_block: i32,
        selection: &AcalaEvmEventSelection,
        log_table: &'static str,
    ) -> Result<Vec<String>, Error> {
        let mut ids: Vec<String> = Vec::new();

        for log in &selection.logs {
            let from_block = format!("{:010}", from_block);
            let to_block = format!("{:010}", to_block + 1);

            let mut params = Parameters::default();
            let mut query = select(["id"])
                .from(log_table)
                .where_(format!("id > {}", params.add(&from_block)))
                .where_(format!("id < {}", params.add(&to_block)));
            if selection.contract != "*" {
                query = query.where_(format!(
                    "event_contract = {}",
                    params.add(&selection.contract)
                ));
            }
            if let Some(contract) = &log.contract {
                query = query.where_(format!("contract = {}", params.add(contract)));
            }
            for idx in 0..=3 {
                if let Some(topics) = log.filter.get(idx) {
                    if !topics.is_empty() {
                        query = query.where_(format!("topic{} = ANY({})", idx, params.add(topics)));
                    }
                }
            }
            query = query.order_by("id");
            let mut log_ids =
                sqlx::query_scalar_with::<_, String, _>(&query.to_string(), params.get())
                    .fetch_all(&self.pool)
                    .observe_duration(log_table)
                    .await?;
            ids.append(&mut log_ids);
        }
        ids.sort();
        ids.dedup();
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
        from_block: i32,
        to_block: i32,
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
                self.query_acala_evm_event(from_block, to_block, selection, event_table)
                    .await?
            } else {
                self.query_acala_evm_event_log(from_block, to_block, selection, log_table)
                    .await?
            };
            ids.append(&mut selection_ids);
        }
        ids.sort();
        ids.dedup();
        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_contracts_events(
        &self,
        from_block: i32,
        to_block: i32,
        selections: &Vec<ContractsEventSelection>,
    ) -> Result<Vec<Event>, Error> {
        if selections.is_empty() {
            return Ok(Vec::new());
        }

        let from_block = format!("{:010}", from_block);
        let to_block = format!("{:010}", to_block + 1);
        let wildcard = selections.iter().any(|selection| selection.contract == "*");
        let contracts = selections
            .iter()
            .map(|selection| selection.contract.clone())
            .collect::<Vec<String>>();

        let mut params = Parameters::default();
        let mut query = select(["event_id"])
            .from("contracts_contract_emitted")
            .where_(format!("event_id > {}", params.add(&from_block)))
            .where_(format!("event_id < {}", params.add(&to_block)));
        if !wildcard {
            query = query.where_(format!("contract = ANY({})", params.add(&contracts)));
        }
        query = query.order_by("event_id");
        let ids = sqlx::query_scalar_with::<_, String, _>(&query.to_string(), params.get())
            .fetch_all(&self.pool)
            .observe_duration("contracts_contract_emitted")
            .await?;

        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    fn group_evm_selections<'a>(
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

    async fn load_evm_logs(
        &self,
        from_block: i32,
        to_block: i32,
        selections: &Vec<EvmLogSelection>,
    ) -> Result<Vec<EvmLog>, Error> {
        if selections.is_empty() {
            return Ok(Vec::new());
        }

        let mut ids = Vec::new();
        for selections in self.group_evm_selections(selections) {
            let from_block = format!("{:010}", from_block);
            let to_block = format!("{:010}", to_block + 1);

            let wildcard = selections.iter().any(|selection| selection.contract == "*");
            let contracts: Vec<String> = selections
                .iter()
                .map(|selection| selection.contract.clone())
                .collect();

            let table = match self.database_type {
                DatabaseType::Cockroach => {
                    let has_topics = if let Some(topics) = selections[0].filter.get(0) {
                        !topics.is_empty()
                    } else {
                        false
                    };
                    if !wildcard && has_topics {
                        "frontier_evm_log@idx_evm_log__contract__topic0__event"
                    } else {
                        "frontier_evm_log"
                    }
                }
                DatabaseType::Postgres => "frontier_evm_log",
            };
            let mut params = Parameters::default();
            let mut query = select(["event_id"])
                .from(table)
                .where_(format!("event_id > {}", params.add(&from_block)))
                .where_(format!("event_id < {}", params.add(&to_block)));
            if !wildcard {
                query = query.where_(&format!(
                    "contract = ANY({}::char(42)[])",
                    params.add(&contracts),
                ));
            }
            for index in 0..=3 {
                if let Some(topics) = selections[0].filter.get(index) {
                    if !topics.is_empty() {
                        query = query.where_(format!(
                            "topic{} = ANY({}::char(66)[])",
                            index,
                            params.add(topics)
                        ));
                    }
                }
            }
            query = query.order_by("event_id");
            let mut log_ids =
                sqlx::query_scalar_with::<_, String, _>(&query.to_string(), params.get())
                    .fetch_all(&self.pool)
                    .observe_duration("frontier_evm_log")
                    .await?;
            ids.append(&mut log_ids);
        }
        ids.sort();
        ids.dedup();
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
                '' AS evm_tx_hash
            FROM event
            WHERE event.id = ANY($1::char(23)[])";
        let mut logs = sqlx::query_as::<_, EvmLog>(query)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;

        let mut extrinsics = logs
            .iter()
            .filter_map(|log| log.extrinsic_id.clone())
            .collect::<Vec<String>>();
        extrinsics.sort();
        extrinsics.dedup();
        let query = "SELECT
                extrinsic_id,
                COALESCE(
                    jsonb_extract_path_text(args, '2'),
                    jsonb_extract_path_text(args, 'transactionHash')
                ) AS evm_tx_hash
            FROM event
            WHERE name = 'Ethereum.Executed' AND extrinsic_id = ANY($1::char(23)[])";
        let tx_hashes = sqlx::query_as::<_, (String, String)>(query)
            .bind(&extrinsics)
            .fetch_all(&self.pool)
            .await?;
        let mut hash_by_extrinsic: HashMap<String, String> = HashMap::new();
        for (extrinsic_id, hash) in tx_hashes {
            hash_by_extrinsic.insert(extrinsic_id, hash);
        }

        for log in &mut logs {
            if let Some(extrinsic_id) = &log.extrinsic_id {
                if let Some(hash) = hash_by_extrinsic.get(extrinsic_id) {
                    log.evm_tx_hash = hash.clone();
                }
            }
        }

        Ok(logs)
    }

    async fn load_eth_transactions(
        &self,
        from_block: i32,
        to_block: i32,
        selections: &Vec<EthTransactSelection>,
    ) -> Result<(Vec<Call>, Vec<Event>), Error> {
        if selections.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut ids = Vec::new();
        for selection in selections {
            let from_block = format!("{:010}", from_block);
            let to_block = format!("{:010}", to_block + 1);

            let mut params = Parameters::default();
            let mut query = select(["call_id"])
                .from("frontier_ethereum_transaction")
                .where_(format!("call_id > {}", params.add(&from_block)))
                .where_(format!("call_id < {}", params.add(&to_block)));
            if selection.contract != "*" {
                query = query.where_(format!("contract = {}", params.add(&selection.contract)));
            }
            if let Some(sighash) = &selection.sighash {
                query = query.where_(format!("sighash = {}", params.add(sighash)));
            }
            query = query.order_by("call_id");

            let sql = query.to_string();
            let mut selection_ids = sqlx::query_scalar_with::<_, String, _>(&sql, params.get())
                .fetch_all(&self.pool)
                .observe_duration("frontier_ethereum_transaction")
                .await?;
            ids.append(&mut selection_ids);
        }
        ids.sort();
        ids.dedup();
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

        let events = if ids.is_empty() {
            vec![]
        } else {
            let query = "SELECT
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
                WHERE call_id = ANY($1::char(30)[]) AND name = 'Ethereum.Executed'";
            sqlx::query_as::<_, Event>(query)
                .bind(&ids)
                .fetch_all(&self.pool)
                .observe_duration("event")
                .await?
        };

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
        Ok((calls, events))
    }

    async fn load_blocks(&self, from_block: i32, to_block: i32) -> Result<Vec<BlockHeader>, Error> {
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
            WHERE height >= $1 AND height <= $2
            ORDER BY height";
        let blocks = sqlx::query_as::<_, BlockHeader>(&query)
            .bind(from_block)
            .bind(to_block)
            .fetch_all(&self.pool)
            .observe_duration("block")
            .await?;
        Ok(blocks)
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
                self.visit_parent_call(parent, &parent_fields, call_lookup, call_fields);
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
