use std::default::Default;

#[derive(Debug, Default)]
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

#[derive(Debug, Default)]
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

    pub fn selected_fields(&self) -> Vec<&str> {
        let mut fields = vec![];
        if self._all {
            fields.push("success");
            fields.push("name");
            fields.push("args");
            fields.push("parent_id");
        } else {
            if self.success {
                fields.push("success");
            }
            if self.name {
                fields.push("name");
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

#[derive(Debug, Default)]
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

    pub fn selected_fields(&self) -> Vec<&str> {
        let mut fields = vec![];
        if self._all {
            fields.push("index_in_block");
            fields.push("signature");
            fields.push("success");
            fields.push("hash");
            fields.push("call_id");
        } else {
            if self.index_in_block {
                fields.push("index_in_block");
            }
            if self.signature {
                fields.push("signature");
            }
            if self.success {
                fields.push("success");
            }
            if self.hash {
                fields.push("hash");
            }
            if self.call.any() {
                fields.push("call_id");
            }
        }
        fields
    }
}

#[derive(Debug, Default)]
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

    pub fn selected_fields(&self) -> Vec<&str> {
        let mut fields = vec![];
        if self._all {
            fields.push("index_in_block");
            fields.push("phase");
            fields.push("extrinsic_id");
            fields.push("call_id");
            fields.push("name");
            fields.push("args");
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
            if self.name {
                fields.push("name");
            }
            if self.args {
                fields.push("args");
            }
        }
        fields
    }
}
