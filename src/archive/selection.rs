#[derive(Debug)]
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

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = vec![];
        if self._all {
            fields.push("name".to_string());
            fields.push("args".to_string());
            fields.push("success".to_string());
            fields.push("parent_id".to_string());
        } else {
            if self.name {
                fields.push("name".to_string());
            }
            if self.args {
                fields.push("args".to_string());
            }
            if self.success {
                fields.push("success".to_string());
            }
            if self.parent {
                fields.push("parent_id".to_string());
            }
        }
        fields
    }
}


#[derive(Debug)]
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

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = vec![];
        if self._all {
            fields.push("success".to_string());
            fields.push("name".to_string());
            fields.push("args".to_string());
            fields.push("parent_id".to_string());
        } else {
            if self.success {
                fields.push("success".to_string());
            }
            if self.name {
                fields.push("name".to_string());
            }
            if self.args {
                fields.push("args".to_string());
            }
            if self.parent.any() {
                fields.push("parent_id".to_string());
            }
        }
        fields
    }
}


#[derive(Debug)]
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

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = vec![];
        if self._all {
            fields.push("index_in_block".to_string());
            fields.push("signature".to_string());
            fields.push("success".to_string());
            fields.push("hash".to_string());
            fields.push("call_id".to_string());
        } else {
            if self.index_in_block {
                fields.push("index_in_block".to_string());
            }
            if self.signature {
                fields.push("signature".to_string());
            }
            if self.success {
                fields.push("success".to_string());
            }
            if self.hash {
                fields.push("hash".to_string());
            }
            if self.call.any() {
                fields.push("call_id".to_string());
            }
        }
        fields
    }
}


#[derive(Debug)]
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

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = vec![];
        if self._all {
            fields.push("index_in_block".to_string());
            fields.push("phase".to_string());
            fields.push("extrinsic_id".to_string());
            fields.push("call_id".to_string());
            fields.push("name".to_string());
            fields.push("args".to_string());
        } else {
            if self.index_in_block {
                fields.push("index_in_block".to_string());
            }
            if self.phase {
                fields.push("phase".to_string());
            }
            if self.extrinsic.any() {
                fields.push("extrinsic_id".to_string());
            }
            if self.call.any() {
                fields.push("call_id".to_string());
            }
            if self.name {
                fields.push("name".to_string());
            }
            if self.args {
                fields.push("args".to_string());
            }
        }
        fields
    }
}


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
        let mut fields = vec![];
        if self.call._all {
            fields.push("success".to_string());
            fields.push("name".to_string());
            fields.push("args".to_string());
            fields.push("parent_id".to_string());
        } else {
            if self.call.success {
                fields.push("success".to_string());
            }
            if self.call.name {
                fields.push("name".to_string());
            }
            if self.call.args {
                fields.push("args".to_string());
            }
            if self.call.parent.any() {
                fields.push("parent_id".to_string());
            }
        }
        if self.extrinsic.any() {
            fields.push("extrinsic_id".to_string());
        }
        fields
    }
}


#[derive(Debug)]
pub struct EvmLogDataSelection {
    pub tx_hash: bool,
    pub substrate: EventDataSelection,
}


impl EvmLogDataSelection {
    pub fn new(value: bool) -> Self {
        EvmLogDataSelection {
            tx_hash: value,
            substrate: EventDataSelection::new(value),
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
