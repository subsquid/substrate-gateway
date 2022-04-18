use crate::archive::ArchiveService;
use crate::entities::{Batch, Metadata, Status};
use crate::metrics::DB_TIME_SPENT_SECONDS;
use crate::archive::cockroach::CockroachArchive;
use crate::archive::selection::{
    ParentCallFields, CallFields, ExtrinsicFields, EventFields,
    EventDataSelection, CallDataSelection, EventSelection, CallSelection,
};
use sqlx::{Pool, Postgres};
use async_graphql::{Context, Object, Result, InputObject};


#[derive(InputObject, Clone, Debug)]
#[graphql(name = "ParentCallFields")]
pub struct ParentCallFieldsInput {
    #[graphql(name="_all")]
    pub _all: Option<bool>,
    pub name: Option<bool>,
    pub args: Option<bool>,
    pub success: Option<bool>,
    pub parent: Option<bool>,
}


impl ParentCallFields {
    pub fn from(fields: ParentCallFieldsInput) -> Self {
        ParentCallFields {
            _all: fields._all.unwrap_or(false),
            name: fields.name.unwrap_or(false),
            args: fields.args.unwrap_or(false),
            success: fields.success.unwrap_or(false),
            parent: fields.parent.unwrap_or(false),
        }
    }
}


#[derive(InputObject, Clone, Debug)]
#[graphql(name = "CallFields")]
pub struct CallFieldsInput {
    #[graphql(name="_all")]
    pub _all: Option<bool>,
    pub success: Option<bool>,
    pub name: Option<bool>,
    pub args: Option<bool>,
    pub parent: Option<ParentCallFieldsInput>,
}


impl CallFields {
    pub fn from(fields: CallFieldsInput) -> Self {
        CallFields {
            _all: fields._all.unwrap_or(false),
            success: fields.success.unwrap_or(false),
            name: fields.name.unwrap_or(false),
            args: fields.args.unwrap_or(false),
            parent: fields.parent.map_or_else(|| {
                ParentCallFields::new(false)
            }, |parent| {
                ParentCallFields::from(parent)
            })
        }
    }
}


#[derive(InputObject, Clone, Debug)]
#[graphql(name = "ExtrinsicFields")]
pub struct ExtrinsicFieldsInput {
    #[graphql(name="_all")]
    pub _all: Option<bool>,
    pub index_in_block: Option<bool>,
    pub signature: Option<bool>,
    pub success: Option<bool>,
    pub hash: Option<bool>,
    pub call: Option<CallFieldsInput>,
}


impl ExtrinsicFields {
    pub fn from(fields: ExtrinsicFieldsInput) -> Self {
        ExtrinsicFields {
            _all: fields._all.unwrap_or(false),
            index_in_block: fields.index_in_block.unwrap_or(false),
            signature: fields.signature.unwrap_or(false),
            success: fields.success.unwrap_or(false),
            hash: fields.hash.unwrap_or(false),
            call: fields.call.map_or_else(|| {
                CallFields::new(false)
            }, |call| {
                CallFields::from(call)
            }),
        }
    }
}


#[derive(InputObject, Clone, Debug)]
#[graphql(name = "EventFields")]
pub struct EventFieldsInput {
    #[graphql(name="_all")]
    pub  _all: Option<bool>,
    pub index_in_block: Option<bool>,
    pub phase: Option<bool>,
    pub extrinsic: Option<ExtrinsicFieldsInput>,
    pub call: Option<CallFieldsInput>,
    pub name: Option<bool>,
    pub args: Option<bool>,
}


impl EventFields {
    pub fn from(fields: EventFieldsInput) -> Self {
        EventFields {
            _all: fields._all.unwrap_or(false),
            index_in_block: fields.index_in_block.unwrap_or(false),
            phase: fields.phase.unwrap_or(false),
            extrinsic: fields.extrinsic.map_or_else(|| {
                ExtrinsicFields::new(false)
            }, |extrinsic| {
                ExtrinsicFields::from(extrinsic)
            }),
            call: fields.call.map_or_else(|| {
                CallFields::new(false)
            }, |call| {
                CallFields::from(call)
            }),
            name: fields.name.unwrap_or(false),
            args: fields.args.unwrap_or(false),
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "EventDataSelection")]
pub struct EventDataSelectionInput {
    pub event: Option<EventFieldsInput>,
}


impl EventDataSelection {
    pub fn from(data: EventDataSelectionInput) -> Self {
        EventDataSelection {
            event: data.event.map_or_else(|| {
                EventFields::new(true)
            }, |event| {
                EventFields::from(event)
            })
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "CallDataSelection")]
pub struct CallDataSelectionInput {
    pub call: Option<CallFieldsInput>,
    pub extrinsic: Option<ExtrinsicFieldsInput>,
}


impl CallDataSelection {
    pub fn from(data: CallDataSelectionInput) -> Self {
        CallDataSelection {
            call: data.call.map_or_else(|| {
                CallFields::new(true)
            }, |call| {
                CallFields::from(call)
            }),
            extrinsic: data.extrinsic.map_or_else(|| {
                ExtrinsicFields::new(true)
            }, |extrinsic| {
                ExtrinsicFields::from(extrinsic)
            }),
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "EventSelection")]
pub struct EventSelectionInput {
    pub name: String,
    pub data: Option<EventDataSelectionInput>,
}


impl EventSelection {
    pub fn from(selection: EventSelectionInput) -> Self {
        EventSelection {
            name: selection.name,
            data: selection.data.map_or_else(|| {
                EventDataSelection::new(true)
            }, |data| {
                EventDataSelection::from(data)
            }),
        }
    }
}


#[derive(InputObject, Clone)]
pub struct CallSelectionInput {
    pub name: String,
    pub data: Option<CallDataSelectionInput>,
}


impl CallSelection {
    pub fn from(selection: CallSelectionInput) -> Self {
        CallSelection {
            name: selection.name,
            data: selection.data.map_or_else(|| {
                CallDataSelection::new(true)
            }, |data| {
                CallDataSelection::from(data)
            }),
        }
    }
}


pub struct QueryRoot;


#[Object]
impl QueryRoot {
    async fn batch(
        &self,
        ctx: &Context<'_>,
        limit: i32,
        #[graphql(default = 0)]
        from_block: i32,
        to_block: Option<i32>,
        #[graphql(name = "events")]
        event_selections: Option<Vec<EventSelectionInput>>,
        #[graphql(name = "calls")]
        call_selections: Option<Vec<CallSelectionInput>>,
        include_all_blocks: Option<bool>,
    ) -> Result<Vec<Batch>> {
        let pool = ctx.data::<Pool<Postgres>>()?;
        let archive = CockroachArchive::new(pool.clone());
        let mut events = Vec::new();
        if let Some(selections) = event_selections {
            for selection in selections {
                events.push(EventSelection::from(selection));
            }
        }
        let mut calls = Vec::new();
        if let Some(selections) = call_selections {
            for selection in selections {
                calls.push(CallSelection::from(selection));
            }
        }
        let include_all_blocks = include_all_blocks.unwrap_or(false);
        let batch = archive.batch(limit, from_block, to_block, &events, &calls, include_all_blocks).await?;
        Ok(batch)
    }

    async fn metadata(&self, ctx: &Context<'_>) -> Result<Vec<Metadata>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let archive = CockroachArchive::new(pool.clone());
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["metadata"]).start_timer();
        let metadata = archive.metadata().await?;
        timer.observe_duration();
        Ok(metadata)
    }

    async fn status(&self, ctx: &Context<'_>) -> Result<Status> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let archive = CockroachArchive::new(pool.clone());
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["block"]).start_timer();
        let status = archive.status().await?;
        timer.observe_duration();
        Ok(status)
    }
}
