use super::fields::{CallFields, EventFields, EvmLogFields, ExtrinsicFields};

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

    pub fn selected_fields(&self) -> Vec<&str> {
        let mut fields = self.call.selected_fields();
        if self.extrinsic.any() {
            fields.push("extrinsic_id");
        }
        fields
    }
}

#[derive(Debug, Clone)]
pub struct EvmLogDataSelection {
    pub event: EvmLogFields,
}

impl EvmLogDataSelection {
    pub fn new(value: bool) -> Self {
        EvmLogDataSelection {
            event: EvmLogFields::new(value),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventSelection {
    pub name: String,
    pub data: EventDataSelection,
}

#[derive(Debug, Clone)]
pub struct CallSelection {
    pub name: String,
    pub data: CallDataSelection,
}

#[derive(Debug, Clone)]
pub struct EvmLogSelection {
    pub contract: String,
    pub filter: Vec<Vec<String>>,
    pub data: EvmLogDataSelection,
}

#[derive(Debug, Clone)]
pub struct EthTransactSelection {
    pub contract: String,
    pub sighash: Option<String>,
    pub data: CallDataSelection,
}

#[derive(Debug, Clone)]
pub struct EthExecutedSelection {
    pub contract: String,
    pub data: EventDataSelection,
}

#[derive(Debug, Clone)]
pub struct ContractsEventSelection {
    pub contract: String,
    pub data: EventDataSelection,
}

#[derive(Debug, Clone)]
pub struct GearMessageEnqueuedSelection {
    pub program: String,
    pub data: EventDataSelection,
}

#[derive(Debug, Clone)]
pub struct GearUserMessageSentSelection {
    pub program: String,
    pub data: EventDataSelection,
}

#[derive(Debug, Clone)]
pub struct AcalaEvmLog {
    pub contract: Option<String>,
    pub filter: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct AcalaEvmEventSelection {
    pub contract: String,
    pub logs: Vec<AcalaEvmLog>,
    pub data: EventDataSelection,
}
