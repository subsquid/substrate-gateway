use crate::entities::{Batch, Event, Call, Extrinsic, BlockHeader};
use crate::repository;
use std::collections::HashMap;
use sqlx::{Error, Postgres, Pool};


pub struct ParentCallFields {
    pub _all: bool,
    pub name: bool,
    pub args: bool,
    pub success: bool,
    pub parent: bool,
}


impl ParentCallFields {
    pub fn new(value: bool) -> Self {
        ParentCallFields {
            _all: value,
            name: value,
            args: value,
            success: value,
            parent: value,
        }
    }

    pub fn any(&self) -> bool {
        if self._all {
            return true;
        }
        if self.name {
            return true;
        }
        if self.args {
            return true;
        }
        if self.success {
            return true;
        }
        if self.parent {
            return true;
        }
        false
    }
}


pub struct CallFields {
    pub _all: bool,
    pub success: bool,
    pub name: bool,
    pub args: bool,
    pub parent: ParentCallFields,
}


impl CallFields {
    pub fn new(value: bool) -> Self {
        CallFields {
            _all: value,
            success: value,
            name: value,
            args: value,
            parent: ParentCallFields::new(value),
        }
    }

    pub fn any(&self) -> bool {
        if self._all {
            return true;
        }
        if self.success {
            return true;
        }
        if self.name {
            return true;
        }
        if self.args {
            return true;
        }
        if self.parent.any() {
            return true;
        }
        false
    }
}


pub struct ExtrinsicFields {
    pub _all: bool,
    pub index_in_block: bool,
    pub signature: bool,
    pub success: bool,
    pub hash: bool,
    pub call: CallFields,
}


impl ExtrinsicFields {
    pub fn new(value: bool) -> Self {
        ExtrinsicFields {
            _all: value,
            index_in_block: value,
            signature: value,
            success: value,
            hash: value,
            call: CallFields::new(value),
        }
    }

    pub fn any(&self) -> bool {
        if self._all {
            return true;
        }
        if self.index_in_block {
            return true;
        }
        if self.signature {
            return true;
        }
        if self.success {
            return true;
        }
        if self.hash {
            return true;
        }
        if self.call.any() {
            return true;
        }
        false
    }
}


pub struct EventFields {
    pub _all: bool,
    pub index_in_block: bool,
    pub phase: bool,
    pub extrinsic: ExtrinsicFields,
    pub call: CallFields,
    pub name: bool,
    pub args: bool,
}


impl EventFields {
    pub fn new(value: bool) -> Self {
        EventFields {
            _all: value,
            index_in_block: value,
            phase: value,
            extrinsic: ExtrinsicFields::new(value),
            call: CallFields::new(value),
            name: value,
            args: value,
        }
    }
}


pub struct EventDataSelection {
    pub event: EventFields,
}


impl EventDataSelection {
    pub fn new(value: bool) -> Self {
        EventDataSelection {
            event: EventFields::new(value),
        }
    }
}


pub struct CallDataSelection {
    pub call: CallFields,
    pub extrinsic: ExtrinsicFields,
}


impl CallDataSelection {
    pub fn new(value: bool) -> Self {
        CallDataSelection {
            call: CallFields::new(value),
            extrinsic: ExtrinsicFields::new(value),
        }
    }
}


pub struct EventSelection {
    pub name: String,
    pub data: EventDataSelection,
}


pub struct CallSelection {
    pub name: String,
    pub data: CallDataSelection,
}


async fn get_blocks(
    pool: &Pool<Postgres>,
    limit: i32,
    from_block: i32,
    to_block: Option<i32>,
    event_selections: &Vec<EventSelection>,
    call_selections: &Vec<CallSelection>,
    include_all_blocks: bool
) -> Result<Vec<BlockHeader>, Error> {
    let events = event_selections.iter()
        .map(|selection| selection.name.clone())
        .collect();
    let calls = call_selections.iter()
        .map(|selection| selection.name.clone())
        .collect();
    let blocks = repository::get_blocks(pool, limit, from_block, to_block, &events, &calls, include_all_blocks).await?;
    Ok(blocks)
}


async fn get_calls(
    pool: &Pool<Postgres>,
    block_ids: &Vec<String>,
    call_selections: &Vec<CallSelection>,
    event_selections: &Vec<EventSelection>
) -> Result<Vec<Call>, Error> {
    let call_names = call_selections.iter()
        .map(|selection| selection.name.clone())
        .collect();
    let event_names = event_selections.iter()
        .filter_map(|selection| {
            if selection.data.event.call.any() {
                return Some(selection.name.clone());
            }
            None
        })
        .collect();
    let mut calls = repository::get_calls(pool, &block_ids, &call_names, &event_names).await?;
    calls.iter_mut().for_each(|call| {
        let selection = call_selections.iter()
            .find(|selection| selection.name == call._name);
        if let Some(selection) = selection {
            if !selection.data.call._all {
                if !selection.data.call.success {
                    call.success = None;
                }
                if !selection.data.call.name {
                    call.name = None;
                }
                if !selection.data.call.args {
                    call.args = None;
                }
                if !selection.data.call.parent.any() {
                    call.parent_id = None;
                }
            }
            if !selection.data.extrinsic.any() {
                call.extrinsic_id = None;
            }
        }
    });
    Ok(calls)
}


async fn get_events(
    pool: &Pool<Postgres>,
    block_ids: &Vec<String>,
    event_selections: &Vec<EventSelection>
) -> Result<Vec<Event>, Error> {
    let names = event_selections.iter().map(|selection| selection.name.clone()).collect();
    let events = repository::get_events(pool, &block_ids, &names).await?;
    Ok(events)
}


async fn get_extrinsics(
    pool: &Pool<Postgres>,
    block_ids: &Vec<String>,
    call_selections: &Vec<CallSelection>,
    event_selections: &Vec<EventSelection>
) -> Result<Vec<Extrinsic>, Error> {
    let calls = call_selections.iter()
        .filter_map(|selection| {
            if selection.data.extrinsic.any() {
                return Some(selection.name.clone());
            }
            None
        })
        .collect();
    let events = event_selections.iter()
        .filter_map(|selection| {
            if selection.data.event.extrinsic.any() {
                return Some(selection.name.clone());
            }
            None
        })
        .collect();
    let extrinsics = repository::get_extrinsics(pool, &block_ids, &calls, &events).await?;
    Ok(extrinsics)
}


async fn get_missed_calls(
    pool: &Pool<Postgres>,
    extrinsics: &Vec<Extrinsic>,
    events: &Vec<Event>,
    calls: &Vec<Call>,
    call_selections: &Vec<CallSelection>,
    event_selections: &Vec<EventSelection>
) -> Result<Vec<Call>, Error> {
    let mut call_ids: Vec<String> = Vec::new();
    for selection in call_selections {
        if selection.data.extrinsic.call.any() {
            let calls_by_name: Vec<&Call> = calls.iter()
                .filter(|call| call._name == selection.name)
                .collect();
            for call in calls_by_name {
                let extrinsic = extrinsics.iter()
                    .find(|extrinsic| call._extrinsic_id == extrinsic.id);
                if let Some(extrinsic) = extrinsic {
                    let call_for_extrinsic = calls.iter().find(|call| extrinsic._call_id == call.id);
                    if call_for_extrinsic.is_none() && !call_ids.contains(&extrinsic._call_id) {
                        call_ids.push(extrinsic._call_id.clone());
                    }
                }
            }
        }
    }
    let calls = repository::get_calls_by_ids(pool, &call_ids).await?;
    Ok(calls)
}


fn create_batch(
    blocks: Vec<BlockHeader>,
    calls: Vec<Call>,
    events: Vec<Event>,
    extrinsics: Vec<Extrinsic>,
    event_selections: &Vec<EventSelection>,
    call_selections: &Vec<CallSelection>,
) -> Vec<Batch> {
    let mut calls_by_block = HashMap::new();
    for mut call in calls {
        calls_by_block.entry(call.block_id.clone())
            .or_insert_with(Vec::new)
            .push(serde_json::to_value(call).unwrap());
    }
    let mut events_by_block = HashMap::new();
    for event in events {
        events_by_block.entry(event.block_id.as_ref().unwrap().clone())
            .or_insert_with(Vec::new)
            .push(serde_json::to_value(event).unwrap());
    }
    let mut extrinsics_by_block = HashMap::new();
    for extrinsic in extrinsics {
        extrinsics_by_block.entry(extrinsic.block_id.as_ref().unwrap().clone())
            .or_insert_with(Vec::new)
            .push(serde_json::to_value(extrinsic).unwrap());
    }
    blocks.into_iter()
        .map(|block| {
            Batch {
                extrinsics: extrinsics_by_block.remove(&block.id),
                calls: calls_by_block.remove(&block.id),
                events: events_by_block.remove(&block.id),
                header: block,
            }
        })
        .collect()
}


pub async fn get_batch(
    pool: &Pool<Postgres>,
    limit: i32,
    from_block: i32,
    to_block: Option<i32>,
    event_selections: Vec<EventSelection>,
    call_selections: Vec<CallSelection>,
    include_all_blocks: bool
) -> Result<Vec<Batch>, Error> {
    let blocks = get_blocks(pool, limit, from_block, to_block, &event_selections, &call_selections, include_all_blocks).await?;
    let block_ids = blocks.iter().map(|block| block.id.clone()).collect();
    let mut calls = get_calls(pool, &block_ids, &call_selections, &event_selections).await?;
    let events = get_events(pool, &block_ids, &event_selections).await?;
    let extrinsics = get_extrinsics(pool, &block_ids, &call_selections, &event_selections).await?;
    let mut missed_calls = get_missed_calls(pool, &extrinsics, &events, &calls, &call_selections, &event_selections).await?;
    calls.append(&mut missed_calls);
    let batch = create_batch(blocks, calls, events, extrinsics, &event_selections, &call_selections);
    Ok(batch)
}
