use substrate_archive::selection::{
    EventDataSelection, CallDataSelection, EventSelection, CallSelection,
    EvmLogDataSelection, EvmLogSelection, ContractsEventSelection, EthTransactSelection,
    GearMessageEnqueuedSelection, GearUserMessageSentSelection, AcalaEvmEventSelection,
    AcalaEvmLog,
};
use substrate_archive::fields::{ParentCallFields, CallFields, ExtrinsicFields, EventFields, EvmLogFields};
use async_graphql::InputObject;

#[derive(InputObject, Clone, Debug)]
#[graphql(name = "ParentCallFields")]
pub struct ParentCallFieldsInput {
    #[graphql(name="_all")]
    pub _all: Option<bool>,
    pub args: Option<bool>,
    pub error: Option<bool>,
    pub origin: Option<bool>,
    pub parent: Option<bool>,
}


impl From<ParentCallFieldsInput> for ParentCallFields {
    fn from(fields: ParentCallFieldsInput) -> Self {
        let _all = fields._all.unwrap_or(false);
        ParentCallFields {
            _all,
            args: _all || fields.args.unwrap_or(false),
            error: _all || fields.error.unwrap_or(false),
            origin: _all || fields.origin.unwrap_or(false),
            parent: _all || fields.parent.unwrap_or(false),
        }
    }
}


#[derive(InputObject, Clone, Debug)]
#[graphql(name = "CallFields")]
pub struct CallFieldsInput {
    #[graphql(name="_all")]
    pub _all: Option<bool>,
    pub error: Option<bool>,
    pub origin: Option<bool>,
    pub args: Option<bool>,
    pub parent: Option<ParentCallFieldsInput>,
}


impl From<CallFieldsInput> for CallFields {
    fn from(fields: CallFieldsInput) -> Self {
        let _all = fields._all.unwrap_or(false);
        CallFields {
            _all,
            error: _all || fields.error.unwrap_or(false),
            origin: _all || fields.origin.unwrap_or(false),
            args: _all || fields.args.unwrap_or(false),
            parent: fields.parent.map_or_else(|| {
                ParentCallFields::new(_all)
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


impl From<ExtrinsicFieldsInput> for ExtrinsicFields {
    fn from(fields: ExtrinsicFieldsInput) -> Self {
        let _all = fields._all.unwrap_or(false);
        ExtrinsicFields {
            _all,
            index_in_block: _all || fields.index_in_block.unwrap_or(false),
            version: _all || fields.version.unwrap_or(false),
            signature: _all || fields.signature.unwrap_or(false),
            success: _all || fields.success.unwrap_or(false),
            error: _all || fields.error.unwrap_or(false),
            hash: _all || fields.hash.unwrap_or(false),
            call: fields.call.map_or_else(|| {
                CallFields::new(_all)
            }, |call| {
                CallFields::from(call)
            }),
            fee: _all || fields.fee.unwrap_or(false),
            tip: _all || fields.tip.unwrap_or(false),
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
    pub args: Option<bool>,
}


impl From<EventFieldsInput> for EventFields {
    fn from(fields: EventFieldsInput) -> Self {
        let _all = fields._all.unwrap_or(false);
        EventFields {
            _all,
            index_in_block: _all || fields.index_in_block.unwrap_or(false),
            phase: _all || fields.phase.unwrap_or(false),
            extrinsic: fields.extrinsic.map_or_else(|| {
                ExtrinsicFields::new(_all)
            }, |extrinsic| {
                ExtrinsicFields::from(extrinsic)
            }),
            call: fields.call.map_or_else(|| {
                CallFields::new(_all)
            }, |call| {
                CallFields::from(call)
            }),
            args: _all || fields.args.unwrap_or(false),
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
    pub args: Option<bool>,
    pub evm_tx_hash: Option<bool>,
}


impl From<EvmLogFieldsInput> for EvmLogFields {
    fn from(fields: EvmLogFieldsInput) -> Self {
        let _all = fields._all.unwrap_or(false);
        EvmLogFields {
            _all,
            index_in_block: _all || fields.index_in_block.unwrap_or(false),
            phase: _all || fields.phase.unwrap_or(false),
            extrinsic: fields.extrinsic.map_or_else(|| {
                ExtrinsicFields::new(_all)
            }, |extrinsic| {
                ExtrinsicFields::from(extrinsic)
            }),
            call: fields.call.map_or_else(|| {
                CallFields::new(_all)
            }, |call| {
                CallFields::from(call)
            }),
            args: _all || fields.args.unwrap_or(false),
            evm_tx_hash: _all || fields.evm_tx_hash.unwrap_or(false),
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "EventDataSelection")]
pub struct EventDataSelectionInput {
    pub event: Option<EventFieldsInput>,
}


impl From<EventDataSelectionInput> for EventDataSelection {
    fn from(data: EventDataSelectionInput) -> Self {
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


impl From<CallDataSelectionInput> for CallDataSelection {
    fn from(data: CallDataSelectionInput) -> Self {
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


impl From<EventSelectionInput> for EventSelection {
    fn from(selection: EventSelectionInput) -> Self {
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


impl From<CallSelectionInput> for CallSelection {
    fn from(selection: CallSelectionInput) -> Self {
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


impl From<EvmLogDataSelectionInput> for EvmLogDataSelection {
    fn from(data: EvmLogDataSelectionInput) -> Self {
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


impl From<EvmLogSelectionInput> for EvmLogSelection {
    fn from(selection: EvmLogSelectionInput) -> Self {
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
#[graphql(name = "EthereumTransactionSelection")]
pub struct EthTransactSelectionInput {
    pub contract: String,
    pub sighash: Option<String>,
    pub data: Option<CallDataSelectionInput>,
}


impl From<EthTransactSelectionInput> for EthTransactSelection {
    fn from(selection: EthTransactSelectionInput) -> Self {
        EthTransactSelection {
            contract: selection.contract,
            sighash: selection.sighash,
            data: selection.data.map_or_else(|| {
                CallDataSelection::new(true)
            }, |data| {
                CallDataSelection::from(data)
            })
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "ContractsEventSelection")]
pub struct ContractsEventSelectionInput {
    pub contract: String,
    pub data: Option<EventDataSelectionInput>,
}


impl From<ContractsEventSelectionInput> for ContractsEventSelection {
    fn from(selection: ContractsEventSelectionInput) -> Self {
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


#[derive(InputObject, Clone)]
#[graphql(name = "GearMessageEnqueuedSelection")]
pub struct GearMessageEnqueuedSelectionInput {
    pub program: String,
    pub data: Option<EventDataSelectionInput>,
}


impl From<GearMessageEnqueuedSelectionInput> for GearMessageEnqueuedSelection {
    fn from(selection: GearMessageEnqueuedSelectionInput) -> Self {
        GearMessageEnqueuedSelection {
            program: selection.program,
            data: selection.data.map_or_else(|| {
                EventDataSelection::new(true)
            }, |data| {
                EventDataSelection::from(data)
            })
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "GearUserMessageSentSelection")]
pub struct GearUserMessageSentSelectionInput {
    pub program: String,
    pub data: Option<EventDataSelectionInput>,
}


impl From<GearUserMessageSentSelectionInput> for GearUserMessageSentSelection {
    fn from(selection: GearUserMessageSentSelectionInput) -> Self {
        GearUserMessageSentSelection {
            program: selection.program,
            data: selection.data.map_or_else(|| {
                EventDataSelection::new(true)
            }, |data| {
                EventDataSelection::from(data)
            })
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "AcalaEvmLog")]
pub struct AcalaEvmLogInput {
    pub contract: Option<String>,
    pub filter: Option<Vec<Vec<String>>>,
}


impl From<AcalaEvmLogInput> for AcalaEvmLog {
    fn from(input: AcalaEvmLogInput) -> Self {
        AcalaEvmLog {
            contract: input.contract,
            filter: input.filter.unwrap_or_default(),
        }
    }
}


#[derive(InputObject, Clone)]
#[graphql(name = "AcalaEvmEventSelection")]
pub struct AcalaEvmEventSelectionInput {
    pub contract: String,
    pub logs: Option<Vec<AcalaEvmLogInput>>,
    pub data: Option<EventDataSelectionInput>,
}


impl From<AcalaEvmEventSelectionInput> for AcalaEvmEventSelection {
    fn from(selection: AcalaEvmEventSelectionInput) -> Self {
        AcalaEvmEventSelection {
            contract: selection.contract,
            logs: selection.logs.map_or_else(Vec::new, |logs| {
                logs.into_iter().map(AcalaEvmLog::from).collect()
            }),
            data: selection.data.map_or_else(|| {
                EventDataSelection::new(true)
            }, |data| {
                EventDataSelection::from(data)
            }),
        }
    }
}
