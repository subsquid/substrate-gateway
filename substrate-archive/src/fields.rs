#[derive(Debug, Clone)]
pub struct ParentCallFields {
    pub _all: bool,
    pub args: bool,
    pub error: bool,
    pub origin: bool,
    pub parent: bool,
}

impl ParentCallFields {
    pub fn new(value: bool) -> Self {
        ParentCallFields {
            _all: value,
            args: value,
            error: value,
            origin: value,
            parent: value,
        }
    }

    pub fn any(&self) -> bool {
        self._all ||
        self.args ||
        self.error ||
        self.origin ||
        self.parent
    }
}

#[derive(Debug, Clone)]
pub struct CallFields {
    pub _all: bool,
    pub error: bool,
    pub origin: bool,
    pub args: bool,
    pub parent: ParentCallFields,
}

impl CallFields {
    pub fn new(value: bool) -> Self {
        CallFields {
            _all: value,
            error: value,
            origin: value,
            args: value,
            parent: ParentCallFields::new(value),
        }
    }

    pub fn any(&self) -> bool {
        self._all ||
        self.error ||
        self.origin ||
        self.args ||
        self.parent.any()
    }

    pub fn selected_fields(&self) -> Vec<&str> {
        let mut fields = vec![];
        if self._all {
            fields.extend_from_slice(&[
                "error",
                "origin",
                "args",
                "parent_id",
            ]);
        } else {
            if self.error {
                fields.push("error");
            }
            if self.origin {
                fields.push("origin");
            }
            if self.args {
                fields.push("args");
            }
            if self.parent.any() {
                fields.push("parent_id");
            }
        }
        fields
    }
}

#[derive(Debug, Clone)]
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

    pub fn selected_fields(&self) -> Vec<&str> {
        let mut fields = vec![];
        if self._all {
            fields.extend_from_slice(&[
                "index_in_block",
                "version",
                "signature",
                "success",
                "error",
                "hash",
                "call_id",
                "fee",
                "tip",
            ]);
        } else {
            if self.index_in_block {
                fields.push("index_in_block");
            }
            if self.version {
                fields.push("version");
            }
            if self.signature {
                fields.push("signature");
            }
            if self.success {
                fields.push("success");
            }
            if self.error {
                fields.push("error");
            }
            if self.hash {
                fields.push("hash");
            }
            if self.call.any() {
                fields.push("call_id");
            }
            if self.fee {
                fields.push("fee");
            }
            if self.tip {
                fields.push("tip");
            }
        }
        fields
    }
}

#[derive(Debug, Clone)]
pub struct EventFields {
    pub _all: bool,
    pub index_in_block: bool,
    pub phase: bool,
    pub extrinsic: ExtrinsicFields,
    pub call: CallFields,
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
            args: value,
        }
    }

    pub fn selected_fields(&self) -> Vec<&str> {
        let mut fields = vec![];
        if self._all {
            fields.extend_from_slice(&[
                "index_in_block",
                "phase",
                "extrinsic_id",
                "call_id",
                "args",
            ]);
        } else {
            if self.index_in_block {
                fields.push("index_in_block");
            }
            if self.phase {
                fields.push("phase");
            }
            if self.extrinsic.any() {
                fields.push("extrinsic_id");
            }
            if self.call.any() {
                fields.push("call_id");
            }
            if self.args {
                fields.push("args");
            }
        }
        fields
    }
}

#[derive(Debug, Clone)]
pub struct EvmLogFields {
    pub _all: bool,
    pub index_in_block: bool,
    pub phase: bool,
    pub extrinsic: ExtrinsicFields,
    pub call: CallFields,
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
            args: value,
            evm_tx_hash: value,
        }
    }

    pub fn selected_fields(&self) -> Vec<&str> {
        let mut fields = vec![];
        if self._all {
            fields.extend_from_slice(&[
                "index_in_block",
                "phase",
                "extrinsic_id",
                "call_id",
                "args",
                "evm_tx_hash",
            ]);
        } else {
            if self.index_in_block {
                fields.push("index_in_block");
            }
            if self.phase {
                fields.push("phase");
            }
            if self.extrinsic.any() {
                fields.push("extrinsic_id");
            }
            if self.call.any() {
                fields.push("call_id");
            }
            if self.args {
                fields.push("args");
            }
            if self.evm_tx_hash {
                fields.push("evm_tx_hash");
            }
        }
        fields
    }
}
