#[derive(Debug)]
pub struct ParentCallFields {
    pub _all: bool,
    pub name: bool,
    pub args: bool,
    pub success: bool,
    pub error: bool,
    pub origin: bool,
    pub parent: bool,
}


impl ParentCallFields {
    pub fn new(value: bool) -> Self {
        ParentCallFields {
            _all: value,
            name: value,
            args: value,
            success: value,
            error: value,
            origin: value,
            parent: value,
        }
    }

    pub fn any(&self) -> bool {
        self._all ||
        self.name ||
        self.args ||
        self.success ||
        self.error ||
        self.origin ||
        self.parent
    }

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = vec![];
        if self._all {
            fields.extend_from_slice(&[
                "name".to_string(),
                "args".to_string(),
                "success".to_string(),
                "error".to_string(),
                "origin".to_string(),
                "parent_id".to_string(),
            ]);
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
            if self.error {
                fields.push("error".to_string());
            }
            if self.origin {
                fields.push("origin".to_string());
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
    pub error: bool,
    pub origin: bool,
    pub name: bool,
    pub args: bool,
    pub parent: ParentCallFields,
}


impl CallFields {
    pub fn new(value: bool) -> Self {
        CallFields {
            _all: value,
            success: value,
            error: value,
            origin: value,
            name: value,
            args: value,
            parent: ParentCallFields::new(value),
        }
    }

    pub fn any(&self) -> bool {
        self._all ||
        self.success ||
        self.error ||
        self.origin ||
        self.name ||
        self.args ||
        self.parent.any()
    }

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = vec![];
        if self._all {
            fields.extend_from_slice(&[
                "success".to_string(),
                "error".to_string(),
                "origin".to_string(),
                "name".to_string(),
                "args".to_string(),
                "parent_id".to_string(),
            ]);
        } else {
            if self.success {
                fields.push("success".to_string());
            }
            if self.error {
                fields.push("error".to_string());
            }
            if self.origin {
                fields.push("origin".to_string());
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
    pub version: bool,
    pub signature: bool,
    pub success: bool,
    pub error: bool,
    pub hash: bool,
    pub call: CallFields,
    pub fee: bool,
    pub tip: bool,
}


impl ExtrinsicFields {
    pub fn new(value: bool) -> Self {
        ExtrinsicFields {
            _all: value,
            index_in_block: value,
            version: value,
            signature: value,
            success: value,
            error: value,
            hash: value,
            call: CallFields::new(value),
            fee: value,
            tip: value,
        }
    }

    pub fn any(&self) -> bool {
        self._all ||
        self.index_in_block ||
        self.version ||
        self.signature ||
        self.success ||
        self.error ||
        self.hash ||
        self.call.any() ||
        self.fee ||
        self.tip
    }

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = vec![];
        if self._all {
            fields.extend_from_slice(&[
                "index_in_block".to_string(),
                "version".to_string(),
                "signature".to_string(),
                "success".to_string(),
                "error".to_string(),
                "hash".to_string(),
                "call_id".to_string(),
                "fee".to_string(),
                "tip".to_string(),
            ]);
        } else {
            if self.index_in_block {
                fields.push("index_in_block".to_string());
            }
            if self.version {
                fields.push("version".to_string());
            }
            if self.signature {
                fields.push("signature".to_string());
            }
            if self.success {
                fields.push("success".to_string());
            }
            if self.error {
                fields.push("error".to_string());
            }
            if self.hash {
                fields.push("hash".to_string());
            }
            if self.call.any() {
                fields.push("call_id".to_string());
            }
            if self.fee {
                fields.push("fee".to_string());
            }
            if self.tip {
                fields.push("tip".to_string());
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
            fields.extend_from_slice(&[
                "index_in_block".to_string(),
                "phase".to_string(),
                "extrinsic_id".to_string(),
                "call_id".to_string(),
                "name".to_string(),
                "args".to_string(),
            ]);
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
pub struct EvmLogFields {
    pub _all: bool,
    pub index_in_block: bool,
    pub phase: bool,
    pub extrinsic: ExtrinsicFields,
    pub call: CallFields,
    pub name: bool,
    pub args: bool,
    pub evm_tx_hash: bool,
}


impl EvmLogFields {
    pub fn new(value: bool) -> Self {
        EvmLogFields {
            _all: value,
            index_in_block: value,
            phase: value,
            extrinsic: ExtrinsicFields::new(value),
            call: CallFields::new(value),
            name: value,
            args: value,
            evm_tx_hash: value,
        }
    }

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = vec![];
        if self._all {
            fields.extend_from_slice(&[
                "index_in_block".to_string(),
                "phase".to_string(),
                "extrinsic_id".to_string(),
                "call_id".to_string(),
                "name".to_string(),
                "args".to_string(),
            ]);
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
