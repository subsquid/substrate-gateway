use crate::archive::fields::{ExtrinsicFields, CallFields};

impl CallFields {
    pub fn merge(&mut self, _fields: &CallFields) {
        
    }
}

impl ExtrinsicFields {
    pub fn merge(&mut self, fields: &ExtrinsicFields) {
        if fields._all {
            self._all = true;
        }
        if fields.index_in_block {
            self.index_in_block = true;
        }
        if fields.version {
            self.version = true;
        }
        if fields.signature {
            self.signature = true;
        }
        if fields.success {
            self.success = true;
        }
        if fields.error {
            self.error = true;
        }
        if fields.hash {
            self.hash = true;
        }
        if fields.call.any() {
            self.call.merge(&fields.call);
        }
        if fields.fee {
            self.fee = true;
        }
        if fields.tip {
            self.tip = true;
        }
    }
}
