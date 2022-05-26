use crate::archive::selection::{
    ParentCallFields, CallFields, ExtrinsicFields, EventFields, EvmLogFields,
    EventDataSelection, CallDataSelection, EventSelection, CallSelection,
    EvmLogDataSelection, EvmLogSelection, ContractsEventSelection,
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
    pub error: Option<bool>,
    pub origin: Option<bool>,
    pub parent: Option<bool>,
}


impl ParentCallFields {
    pub fn from(fields: ParentCallFieldsInput) -> Self {
        ParentCallFields {
            _all: fields._all.unwrap_or(false),
            name: fields.name.unwrap_or(false),
            args: fields.args.unwrap_or(false),
            success: fields.success.unwrap_or(false),
            error: fields.error.unwrap_or(false),
            origin: fields.origin.unwrap_or(false),
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
    pub error: Option<bool>,
    pub origin: Option<bool>,
    pub name: Option<bool>,
    pub args: Option<bool>,
    pub parent: Option<ParentCallFieldsInput>,
}


impl CallFields {
    pub fn from(fields: CallFieldsInput) -> Self {
        CallFields {
            _all: fields._all.unwrap_or(false),
            success: fields.success.unwrap_or(false),
            error: fields.error.unwrap_or(false),
            origin: fields.origin.unwrap_or(false),
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
    pub version: Option<bool>,
    pub signature: Option<bool>,
    pub success: Option<bool>,
    pub error: Option<bool>,
    pub hash: Option<bool>,
    pub call: Option<CallFieldsInput>,
    pub fee: Option<bool>,
    pub tip: Option<bool>,
}


impl ExtrinsicFields {
    pub fn from(fields: ExtrinsicFieldsInput) -> Self {
        ExtrinsicFields {
            _all: fields._all.unwrap_or(false),
            index_in_block: fields.index_in_block.unwrap_or(false),
            version: fields.version.unwrap_or(false),
            signature: fields.signature.unwrap_or(false),
            success: fields.success.unwrap_or(false),
            error: fields.error.unwrap_or(false),
            hash: fields.hash.unwrap_or(false),
            call: fields.call.map_or_else(|| {
                CallFields::new(false)
            }, |call| {
                CallFields::from(call)
            }),
            fee: fields.fee.unwrap_or(false),
            tip: fields.tip.unwrap_or(false),
        }
    }
}


#[derive(InputObject, Clone, Debug)]
#[graphql(name = "EventFields")]
pub struct EventFieldsInput {
    #[graphql(name="_all")]
    pub _all: Option<bool>,
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


#[derive(InputObject, Clone, Debug)]
#[graphql(name = "EvmLogFields")]
pub struct EvmLogFieldsInput {
    #[graphql(name="_all")]
    pub _all: Option<bool>,
    pub index_in_block: Option<bool>,
    pub phase: Option<bool>,
    pub extrinsic: Option<ExtrinsicFieldsInput>,
    pub call: Option<CallFieldsInput>,
    pub name: Option<bool>,
    pub args: Option<bool>,
    pub evm_tx_hash: Option<bool>,
}


impl EvmLogFields {
    pub fn from(fields: EvmLogFieldsInput) -> Self {
        EvmLogFields {
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
            evm_tx_hash: fields.evm_tx_hash.unwrap_or(false),
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


#[derive(InputObject, Clone)]
#[graphql(name = "EvmLogDataSelection")]
pub struct EvmLogDataSelectionInput {
    pub event: Option<EvmLogFieldsInput>,
}


impl EvmLogDataSelection {
    pub fn from(data: EvmLogDataSelectionInput) -> Self {
        EvmLogDataSelection {
            event: data.event.map_or_else(|| {
                EvmLogFields::new(true)
            }, |event| {
                EvmLogFields::from(event)
            }),
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "EvmLogSelection")]
pub struct EvmLogSelectionInput {
    pub contract: String,
    pub filter: Option<Vec<Vec<String>>>,
    pub data: Option<EvmLogDataSelectionInput>,
}


impl EvmLogSelection {
    pub fn from(selection: EvmLogSelectionInput) -> Self {
        EvmLogSelection {
            contract: selection.contract,
            filter: selection.filter.unwrap_or_default(),
            data: selection.data.map_or_else(|| {
                EvmLogDataSelection::new(true)
            }, |data| {
                EvmLogDataSelection::from(data)
            }),
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "ContractsEventSelection")]
pub struct ContractsEventSelectionInput {
    pub contract: String,
    pub data: Option<EventDataSelectionInput>,
}

impl ContractsEventSelection {
    pub fn from(selection: ContractsEventSelectionInput) -> Self {
        ContractsEventSelection {
            contract: selection.contract,
            data: selection.data.map_or_else(|| {
                EventDataSelection::new(true)
            }, |data| {
                EventDataSelection::from(data)
            })
        }
    }
}
