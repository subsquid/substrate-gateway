use std::collections::{HashMap, HashSet};
use sqlx::{Pool, Arguments};
use sqlx::postgres::{PgArguments, Postgres};
use crate::entities::{Batch, Event, BlockHeader, EvmLog, ContractsEvent, Call, Extrinsic};
use crate::error::Error;
use crate::fields::{ExtrinsicFields, EventFields, CallFields};
use crate::selection::{
    CallSelection, CallDataSelection, EventSelection,
    EvmLogSelection, ContractsEventSelection, EthTransactSelection,
    GearMessageEnqueuedSelection, GearUserMessageSentSelection,
    EventDataSelection, EvmExecutedSelection
};
use crate::metrics::ObserverExt;
use super::BatchOptions;
use super::utils::unify_and_merge;
use super::serializer::{CallSerializer, EventSerializer, ExtrinsicSerializer};

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
        pos::int8,
        contract
    FROM event
    WHERE id = ANY($1::char(23)[])";

impl<'a> BatchLoader<'a> {
    pub async fn load(&self) -> Result<Vec<Batch>, Error> {
        let mut calls = self.load_calls().await?;
        let mut events = self.load_events().await?;
        let mut evm_logs = self.get_evm_logs().await?;
        let mut eth_transactions = self.load_eth_transactions().await?;
        let mut contracts_events = self.get_contracts_events().await?;
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
                    if let Some(value) = event.data.get("extrinsic_id") {
                        if let Some(extrinsic_id) = value.as_str() {
                            extrinsic_fields.entry(extrinsic_id.to_string())
                                .and_modify(|fields| fields.merge(&selection.data.event.extrinsic))
                                .or_insert_with(|| selection.data.event.extrinsic.clone());
                        }
                    }
                    if let Some(value) = event.data.get("call_id") {
                        if let Some(call_id) = value.as_str() {
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
        }
        for log in &evm_logs {
            let selection = &self.evm_log_selections[log.selection_index as usize];
            if let Some(value) = log.data.get("extrinsic_id") {
                if let Some(extrinsic_id) = value.as_str() {
                    if selection.data.event.extrinsic.any() {
                        extrinsic_fields.entry(extrinsic_id.to_string())
                            .and_modify(|fields| fields.merge(&selection.data.event.extrinsic))
                            .or_insert_with(|| selection.data.event.extrinsic.clone());
                    }
                }
            }
            if let Some(value) = log.data.get("call_id") {
                if let Some(call_id) = value.as_str() {
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
                                      evm_logs, contracts_events);
        Ok(batch)
    }

    async fn load_calls(&self) -> Result<Vec<Call>, Error> {
        if self.call_selections.is_empty() {
            return Ok(Vec::new());
        }
        let from_block = format!("{:010}", self.from_block);
        let to_block = self.to_block.map(|to_block| format!("{:010}", to_block + 1));

        let mut wildcard = false;
        let mut names = Vec::new();
        for selection in self.call_selections {
            if selection.name == "*" {
                wildcard = true;
            } else {
                names.push(selection.name.clone());
            }
        }

        let id_query = Box::new(CallIdQuery { wildcard, names, from_block, to_block });
        let ids = self.load_ids(id_query).await?;

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

    async fn load_calls_by_ids(&self, ids: &Vec<String>) -> Result<Vec<Call>, Error> {
        let query = "WITH RECURSIVE recursive_call AS (
                SELECT * FROM call WHERE id = ANY($1::varchar(30)[])
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
            ORDER BY block_id";
        let calls = sqlx::query_as::<_, Call>(query)
            .bind(ids)
            .fetch_all(&self.pool)
            .observe_duration("call")
            .await?;
        Ok(calls)
    }

    async fn load_events(&self) -> Result<Vec<Event>, Error> {
        if self.event_selections.is_empty() {
            return Ok(Vec::new());
        }
        let mut wildcard = false;
        let mut names = Vec::new();
        for selection in self.event_selections {
            if selection.name == "*" {
                wildcard = true;
            } else {
                names.push(selection.name.clone());
            }
        }
        let from_block = format!("{:010}", self.from_block);
        let to_block = self.to_block.map(|to_block| format!("{:010}", to_block + 1));

        let id_query = Box::new(EventIdQuery { wildcard, names, from_block, to_block });
        let ids = self.load_ids(id_query).await?;
        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn load_ids(&self, query: Box<dyn IdQuery + Send>) -> Result<Vec<String>, Error> {
        let mut ids = Vec::new();
        let mut blocks = HashSet::new();
        let mut offset: i64 = 0;
        let limit: i64 = 5000;

        'outer: loop {
            let fut = query.execute(offset, limit, &self.pool);
            let result = fut.await?;
            if result.is_empty() {
                break
            } else {
                for (id,) in result {
                    let block_id = id.split('-').next().unwrap().to_string();
                    if blocks.len() == self.limit as usize && !blocks.contains(&block_id) {
                        break 'outer;
                    }
                    blocks.insert(block_id);
                    ids.push(id);
                }
                offset += limit;
            }
        }
        Ok(ids)
    }

    async fn load_messages_enqueued(&self) -> Result<Vec<Event>, Error> {
        if self.gear_message_enqueued_selections.is_empty() {
            return Ok(Vec::new())
        }
        let from_block = format!("{:010}", self.from_block);
        let to_block = self.to_block.map(|to_block| format!("{:010}", to_block + 1));
        let programs = self.gear_message_enqueued_selections.iter()
            .map(|selection| selection.program.clone())
            .collect::<Vec<String>>();
        let id_query = Box::new(MessageEnqueuedIdQuery { programs, to_block, from_block });
        let ids = self.load_ids(id_query).await?;
        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
            .await?;
        Ok(events)
    }

    async fn load_messages_sent(&self) -> Result<Vec<Event>, Error> {
        if self.gear_user_message_sent_selections.is_empty() {
            return Ok(Vec::new())
        }
        let from_block = format!("{:010}", self.from_block);
        let to_block = self.to_block.map(|to_block| format!("{:010}", to_block + 1));
        let programs = self.gear_user_message_sent_selections.iter()
            .map(|selection| selection.program.clone())
            .collect::<Vec<String>>();
        let id_query = Box::new(MessageSentIdQuery { programs, to_block, from_block });
        let ids = self.load_ids(id_query).await?;
        let events = sqlx::query_as::<_, Event>(EVENTS_BY_ID_QUERY)
            .bind(&ids)
            .fetch_all(&self.pool)
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

    async fn get_contracts_events(&self) -> Result<Vec<ContractsEvent>, Error> {
        if self.contracts_event_selections.is_empty() {
            return Ok(Vec::new());
        }
        let query_dynamic_part = self.contracts_event_selections.iter()
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
                let from_block = format!("{:010}", self.from_block);
                let to_block = self.to_block.map_or(
                    "null".to_string(),
                    |to_block| format!("{:010}", to_block + 1)
                );
                let contract_condition = if selection.contract == "*" {
                    "event.name = 'Contracts.ContractEmitted'".to_string()
                } else {
                    format!("event.contract = '{}'", &selection.contract)
                };
                format!(
                    "(
                        SELECT
                            event.block_id,
                            json_agg(jsonb_build_object(
                                'selection_index', {},
                                'data', jsonb_build_object({})
                            )) as events
                        FROM event
                        WHERE {} AND event.block_id >= '{}'
                            AND ({} IS null OR event.block_id < '{}')
                        GROUP BY block_id
                        ORDER BY block_id
                        LIMIT {}
                    )",
                    index, &build_object_fields, &contract_condition,
                    from_block, to_block, to_block, self.limit
                )
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
            ) AS events_by_block", &query_dynamic_part, self.limit);
        let events = sqlx::query_as::<_, ContractsEvent>(&query)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(events)
    }

    async fn get_evm_logs(&self) -> Result<Vec<EvmLog>, Error> {
        if self.evm_log_selections.is_empty() {
            return Ok(Vec::new());
        }
        let query = self.evm_log_selections.iter()
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
                let from_block = format!("{:010}", self.from_block);
                let to_block = self.to_block.map_or(
                    "null".to_string(),
                    |to_block| format!("{:010}", to_block + 1)
                );
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
                        {}::int2 AS selection_index,
                        jsonb_build_object({}) AS data
                    FROM event
                    INNER JOIN event executed_event
                        ON event.extrinsic_id = executed_event.extrinsic_id
                            AND executed_event.name = 'Ethereum.Executed'
                    WHERE {} AND event.block_id IN (
                        SELECT DISTINCT block_id FROM event
                        WHERE {} AND block_id > '{}' AND ({} IS NULL OR block_id < '{}')
                        ORDER BY block_id
                        LIMIT {}
                    )
                )", index, &build_object_fields, &conditions, &conditions, from_block, to_block, to_block, self.limit)
            })
            .collect::<Vec<String>>()
            .join(" UNION ");
        let logs = sqlx::query_as::<_, EvmLog>(&query)
            .fetch_all(&self.pool)
            .observe_duration("event")
            .await?;
        Ok(logs)
    }

    async fn load_eth_transactions(&self) -> Result<Vec<Call>, Error> {
        if self.eth_transact_selections.is_empty() {
            return Ok(Vec::new());
        }
        let from_block = format!("{:010}", self.from_block);
        let to_block = self.to_block.map(|to_block| format!("{:010}", to_block + 1));
        let contracts = self.eth_transact_selections.iter()
            .map(|selection| selection.contract.clone())
            .collect::<Vec<String>>();
        let query = "WITH RECURSIVE recursive_call AS (
                SELECT * FROM call WHERE contract = ANY($1) AND block_id IN (
                    SELECT DISTINCT block_id FROM call
                    WHERE contract = ANY($1)
                        AND block_id > $2 AND ($3 IS null OR block_id < $3)
                    ORDER BY block_id
                    LIMIT $4
                )
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
            ORDER BY block_id";
        let transactions = sqlx::query_as::<_, Call>(&query)
            .bind(contracts)
            .bind(from_block)
            .bind(to_block)
            .bind(self.limit)
            .fetch_all(&self.pool)
            .observe_duration("call")
            .await?;
        Ok(transactions)
    }

    fn get_block_ids(
        &self,
        calls: &Vec<Call>,
        events: &Vec<Event>,
        evm_logs: &Vec<EvmLog>,
        eth_transactions: &Vec<Call>,
        contracts_events: &Vec<ContractsEvent>,
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
        evm_logs: Vec<EvmLog>,
        contracts_events: Vec<ContractsEvent>,
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


#[async_trait::async_trait]
trait IdQuery {
    async fn execute(
        &self,
        offset: i64,
        limit: i64,
        pool: &Pool<Postgres>
    ) -> Result<Vec<(String,)>, Error>;
}

struct EventIdQuery {
    wildcard: bool,
    names: Vec<String>,
    from_block: String,
    to_block: Option<String>,
}

#[async_trait::async_trait]
impl IdQuery for EventIdQuery {
    async fn execute(
        &self,
        offset: i64,
        limit: i64,
        pool: &Pool<Postgres>
    ) -> Result<Vec<(String,)>, Error> {
        let query = "SELECT id
            FROM event
            WHERE ($1 OR name = ANY($2))
                AND block_id > $3 AND ($4 IS null OR block_id < $4)
            ORDER BY block_id, id
            OFFSET $5
            LIMIT $6";
        let result = sqlx::query_as::<_, (String,)>(query)
            .bind(self.wildcard)
            .bind(&self.names)
            .bind(&self.from_block)
            .bind(&self.to_block)
            .bind(offset)
            .bind(limit)
            .fetch_all(pool)
            .observe_duration("event")
            .await?;
        Ok(result)
    }
}

struct CallIdQuery {
    wildcard: bool,
    names: Vec<String>,
    from_block: String,
    to_block: Option<String>,
}

#[async_trait::async_trait]
impl IdQuery for CallIdQuery {
    async fn execute(
        &self,
        offset: i64,
        limit: i64,
        pool: &Pool<Postgres>
    ) -> Result<Vec<(String,)>, Error> {
        let query = "SELECT id
            FROM call
            WHERE ($1 OR name = ANY($2))
                AND block_id > $3 AND ($4 IS null OR block_id < $4)
            ORDER BY block_id, id
            OFFSET $5
            LIMIT $6";
        let result = sqlx::query_as::<_, (String,)>(query)
            .bind(self.wildcard)
            .bind(&self.names)
            .bind(&self.from_block)
            .bind(&self.to_block)
            .bind(offset)
            .bind(limit)
            .fetch_all(pool)
            .observe_duration("call")
            .await?;
        Ok(result)
    }
}

struct MessageEnqueuedIdQuery {
    programs: Vec<String>,
    from_block: String,
    to_block: Option<String>,
}

#[async_trait::async_trait]
impl IdQuery for MessageEnqueuedIdQuery {
    async fn execute(
        &self,
        offset: i64,
        limit: i64,
        pool: &Pool<Postgres>
    ) -> Result<Vec<(String,)>, Error> {
        let query = "SELECT id
            FROM event
            WHERE name = 'Gear.MessageEnqueued' AND contract = ANY($1)
                AND block_id > $2 AND ($3 IS null OR block_id < $3)
            ORDER BY block_id, id
            OFFSET $4
            LIMIT $5";
        let result = sqlx::query_as::<_, (String,)>(query)
            .bind(&self.programs)
            .bind(&self.from_block)
            .bind(&self.to_block)
            .bind(offset)
            .bind(limit)
            .fetch_all(pool)
            .observe_duration("event")
            .await?;
        Ok(result)
    }
}

struct MessageSentIdQuery {
    programs: Vec<String>,
    from_block: String,
    to_block: Option<String>,
}

#[async_trait::async_trait]
impl IdQuery for MessageSentIdQuery {
    async fn execute(
        &self,
        offset: i64,
        limit: i64,
        pool: &Pool<Postgres>
    ) -> Result<Vec<(String,)>, Error> {
        let query = "SELECT id
            FROM event
            WHERE name = 'Gear.UserMessageSent' AND contract = ANY($1)
                AND block_id > $2 AND ($3 IS null OR block_id < $3)
            ORDER BY block_id
            OFFSET $4
            LIMIT $5";
        let result = sqlx::query_as::<_, (String,)>(query)
            .bind(&self.programs)
            .bind(&self.from_block)
            .bind(&self.to_block)
            .bind(offset)
            .bind(limit)
            .fetch_all(pool)
            .observe_duration("event")
            .await?;
        Ok(result)
    }
}
