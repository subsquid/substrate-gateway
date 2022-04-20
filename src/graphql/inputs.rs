use crate::archive::selection::{
    ParentCallFields, CallFields, ExtrinsicFields, EventFields,
    EventDataSelection, CallDataSelection, EventSelection, CallSelection,
};
use async_graphql::InputObject;

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
