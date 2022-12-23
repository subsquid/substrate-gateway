use crate::entities::{Call, Event, EvmLog};
use crate::selection::{
    AcalaEvmEventSelection, AcalaEvmLog, CallSelection, ContractsEventSelection,
    EthTransactSelection, EventSelection, EvmLogSelection, GearMessageEnqueuedSelection,
    GearUserMessageSentSelection,
};
use serde_json::Value;

const WILDCARD: &str = "*";

impl CallSelection {
    pub fn r#match(&self, call: &Call) -> bool {
        self.name == WILDCARD || self.name == call.name
    }
}

impl EventSelection {
    pub fn r#match(&self, event: &Event) -> bool {
        self.name == WILDCARD || self.name == event.name
    }
}

impl EvmLogSelection {
    pub fn r#match(&self, log: &EvmLog) -> bool {
        if let Some(args) = &log.args {
            if let Some(address) = self.get_address(args) {
                return self.contract_match(address) && self.filter_match(log);
            }
        }

        false
    }

    fn contract_match(&self, address: &str) -> bool {
        self.contract == WILDCARD || self.contract == address
    }

    fn filter_match(&self, log: &EvmLog) -> bool {
        let filter: Vec<_> = self.filter.iter().enumerate().collect();
        for (index, topics) in filter {
            if !self.topics_match(topics, log, index) {
                return false;
            }
        }
        true
    }

    fn topics_match(&self, topics: &Vec<String>, log: &EvmLog, index: usize) -> bool {
        if topics.is_empty() {
            return true;
        }

        if let Some(log_topic) = self.get_log_topic(log, index) {
            topics.iter().any(|topic| topic == &log_topic)
        } else {
            false
        }
    }

    fn get_log_topic(&self, log: &EvmLog, index: usize) -> Option<String> {
        if let Some(value) = &log.args {
            if let Some(value) = value.get("topics") {
                if let Some(value) = value.get(index) {
                    if let Some(topic) = value.as_str() {
                        return Some(topic.to_string());
                    }
                }
            }
            if let Some(value) = value.get("log") {
                if let Some(value) = value.get("topics") {
                    if let Some(value) = value.get(index) {
                        if let Some(topic) = value.as_str() {
                            return Some(topic.to_string());
                        }
                    }
                }
            }
        }
        None
    }

    fn get_address<'a>(&'a self, args: &'a Value) -> Option<&str> {
        if let Some(value) = args.get("address") {
            if let Some(address) = value.as_str() {
                return Some(address);
            }
        }
        if let Some(log) = args.get("log") {
            if let Some(value) = log.get("address") {
                if let Some(address) = value.as_str() {
                    return Some(address);
                }
            }
        }
        None
    }
}

impl EthTransactSelection {
    pub fn r#match(&self, call: &Call) -> bool {
        if let Some(args) = &call.args {
            if let Some(transaction) = args.get("transaction") {
                if let Some(address) = self.get_transaction_address(transaction) {
                    return self.contract_match(address) && self.sighash_match(transaction);
                }
            }
        }
        false
    }

    fn sighash_match(&self, args: &Value) -> bool {
        if let Some(sighash) = &self.sighash {
            if let Some(value) = args.get("input") {
                if let Some(input) = value.as_str() {
                    return sighash == &input[..10].to_string();
                }
                {
                    return false;
                }
            }
            return false;
        }
        true
    }

    fn contract_match(&self, address: &str) -> bool {
        self.contract == WILDCARD || self.contract == address
    }

    fn get_transaction_address<'a>(&'a self, transaction: &'a Value) -> Option<&str> {
        let action = transaction.get("action").or_else(|| {
            transaction
                .get("value")
                .and_then(|value| value.get("action"))
        });
        if let Some(action) = action {
            if let Some(value) = action.get("value") {
                if let Some(value) = value.as_str() {
                    return Some(value);
                }
            }
        }
        None
    }
}

impl ContractsEventSelection {
    pub fn r#match(&self, event: &Event) -> bool {
        if let Some(value) = &event.args {
            if let Some(value) = value.get("contract") {
                if let Some(contract) = value.as_str() {
                    return self.contract == WILDCARD || contract == self.contract;
                }
            }
        }
        false
    }
}

impl GearMessageEnqueuedSelection {
    pub fn r#match(&self, event: &Event) -> bool {
        if let Some(value) = &event.args {
            if let Some(value) = value.get("destination") {
                if let Some(destination) = value.as_str() {
                    return destination == self.program;
                }
            }
        }
        false
    }
}

impl GearUserMessageSentSelection {
    pub fn r#match(&self, event: &Event) -> bool {
        if let Some(value) = &event.args {
            if let Some(value) = value.get("message") {
                if let Some(value) = value.get("source") {
                    if let Some(source) = value.as_str() {
                        return source == self.program;
                    }
                }
            }
        }
        false
    }
}

impl AcalaEvmEventSelection {
    pub fn r#match(&self, event: &Event) -> bool {
        if let Some(value) = &event.args {
            if self.contract_match(value) {
                return self.logs_match(value);
            }
        }
        false
    }

    fn contract_match(&self, args: &Value) -> bool {
        if self.contract == WILDCARD {
            return true;
        }

        if let Some(value) = args.get("contract") {
            if let Some(contract) = value.as_str() {
                return self.contract == contract;
            }
        }
        false
    }

    fn logs_match(&self, args: &Value) -> bool {
        if self.logs.is_empty() {
            return true;
        }

        self.logs.iter().any(|log| self.log_match(log, args))
    }

    fn is_log_empty(&self, log: &AcalaEvmLog) -> bool {
        if log.contract.is_some() {
            return false;
        }
        for topics in &log.filter {
            if !topics.is_empty() {
                return false;
            }
        }
        true
    }

    fn log_match(&self, log: &AcalaEvmLog, args: &Value) -> bool {
        if self.is_log_empty(log) {
            return true;
        }

        if let Some(value) = args.get("logs") {
            if let Some(logs) = value.as_array() {
                for nested_log in logs {
                    if let Some(contract) = &log.contract {
                        let address = nested_log
                            .get("address")
                            .and_then(|address| address.as_str());
                        if let Some(address) = address {
                            if address != contract {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }

                    let topics_match = log.filter.iter().enumerate().all(|(index, topics)| {
                        topics.iter().any(|topic| {
                            if let Some(value) = nested_log.get("topics") {
                                if let Some(topics) = value.as_array() {
                                    if let Some(value) = topics.get(index) {
                                        if let Some(log_topic) = value.as_str() {
                                            return log_topic == topic;
                                        }
                                    }
                                }
                            }
                            false
                        })
                    });
                    if topics_match {
                        return true;
                    }
                }
            }
        }
        false
    }
}
