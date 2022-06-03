use super::fields::{EventFields, CallFields, ExtrinsicFields, EvmLogFields};


#[derive(Debug)]
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


#[derive(Debug)]
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

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = self.call.selected_fields();
        if self.extrinsic.any() {
            fields.push("extrinsic_id".to_string());
        }
        fields
    }
}


#[derive(Debug)]
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


#[derive(Debug)]
pub struct EventSelection {
    pub name: String,
    pub data: EventDataSelection,
}


#[derive(Debug)]
pub struct CallSelection {
    pub name: String,
    pub data: CallDataSelection,
}


#[derive(Debug)]
pub struct EvmLogSelection {
    pub contract: String,
    pub filter: Vec<Vec<String>>,
    pub data: EvmLogDataSelection,
}

#[derive(Debug)]
pub struct ContractsEventSelection {
    pub contract: String,
    pub data: EventDataSelection,
}
