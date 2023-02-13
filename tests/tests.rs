use common::{launch_gateway, Client};
use serde_json::json;

mod common;

#[actix_web::test]
async fn test_parent_call_loaded() {
    launch_gateway();
    let client = Client::new();
    let selections_data = [json!({"_all": true}), json!({"parent": {"_all": true}})];
    for selection_data in selections_data {
        let batch = client
            .batch(json!({
                "calls": [{
                    "name": "Balances.transfer",
                    "data": {"call": selection_data},
                }]
            }))
            .await;
        let requested_call = batch
            .calls
            .iter()
            .find(|call| call.id == "0000650677-000003-0f08a-000001");
        let parent_call = batch
            .calls
            .iter()
            .find(|call| call.id == "0000650677-000003-0f08a");
        assert!(requested_call.is_some());
        assert!(parent_call.is_some());
    }
}

#[actix_web::test]
async fn test_parent_call_skipped() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "calls": [{
                "name": "Balances.transfer",
                "data": {
                    "call": {
                        "parent": {"_all": false}
                    },
                    "extrinsic": {"_all": false}
                }
            }]
        }))
        .await;
    let requested_call = batch
        .calls
        .iter()
        .find(|call| call.id == "0000650677-000003-0f08a-000001");
    let parent_call = batch
        .calls
        .iter()
        .find(|call| call.id == "0000650677-000003-0f08a");
    assert!(requested_call.is_some());
    assert!(parent_call.is_none());
}

#[actix_web::test]
async fn test_returned_calls_are_unique() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "calls": [{"name": "Ethereum.transact"}],
            "ethereumTransactions": [{"contract": "0xb654611f84a8dc429ba3cb4fda9fad236c505a1a"}],
        }))
        .await;
    let call = &batch.calls[0];
    assert!(batch.calls.len() == 1);
    assert!(call.id == "0000569006-000018-5e412");
}

#[actix_web::test]
async fn test_event_loads_related_call() {
    launch_gateway();
    let client = Client::new();
    let selections = [
        json!({"name": "Contracts.ContractEmitted"}),
        json!({
            "name": "Contracts.ContractEmitted",
            "data": {"event": {"_all": true}},
        }),
    ];
    for selection in selections {
        let batch = client.batch(json!({ "events": json!([selection]) })).await;
        let call = &batch.calls[0];
        assert!(call.id == "0000000734-000001-251d1");
    }
}

#[actix_web::test]
async fn test_evm_log_has_tx_hash() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "evmLogs": [{
                "contract": "0xb654611f84a8dc429ba3cb4fda9fad236c505a1a",
                "filter": [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]],
                "data": {
                    "event": {
                        "evmTxHash": true,
                    }
                }
            }]
        }))
        .await;
    let log = &batch.events[0];
    let tx_hash = "0x8eafe131eee90e0dfb07d6df46b1aea737834936968da31f807af566a59148b9".to_string();
    assert!(log.id == "0000569006-000084-5e412");
    assert!(log.evmTxHash == Some(tx_hash));
}

#[actix_web::test]
async fn test_ethereum_transactions() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "ethereumTransactions": [{
                "contract": "0xb654611f84a8dc429ba3cb4fda9fad236c505a1a"
            }]
        }))
        .await;
    let call = &batch.calls[0];
    let executed = &batch.events[0];
    assert!(call.id == "0000569006-000018-5e412");
    assert!(executed.id == "0000569006-000085-5e412");
}

#[actix_web::test]
async fn test_ethereum_transactions_wildcard_search() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "ethereumTransactions": [{
                "contract": "*",
                "sighash": "0xd0def521"
            }]
        }))
        .await;
    let call = &batch.calls[0];
    let executed = &batch.events[0];
    assert!(call.id == "0000569006-000018-5e412");
    assert!(executed.id == "0000569006-000085-5e412");
}

#[actix_web::test]
async fn test_contracts_events() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "contractsEvents": [{
                "contract": "0xf98402623dbe32d22b67e0a25136b763615c14fd4201b1aac8832ec52aa64d10",
                "data": {
                    "event": {
                        "_all": true
                    }
                }
            }]
        }))
        .await;
    let event = &batch.events[0];
    assert!(event.id == "0000000734-000004-251d1");
}

#[actix_web::test]
async fn test_wildcard_search() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "fromBlock": 734,
            "calls": [{"name": "*"}],
            "events": [{"name": "*"}],
        }))
        .await;
    let event = &batch.events[0];
    assert!(event.id == "0000000734-000004-251d1");
    assert!(event.name == "Contracts.ContractEmitted");
    let call = &batch.calls[0];
    assert!(call.id == "0000000734-000001-251d1");
    assert!(call.name == "Contracts.call");
}

#[actix_web::test]
async fn test_evm_wildcard_search() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "evmLogs": [{"contract": "*"}],
        }))
        .await;
    let event = &batch.events[0];
    assert!(event.id == "0000569006-000084-5e412");
    assert!(event.name == "EVM.Log");
}

#[actix_web::test]
async fn test_gear_messages_enqueued() {
    launch_gateway();
    let client = Client::new();
    let batch = client.batch(json!({
        "gearMessagesEnqueued": [{"program": "0x5466a0b28225bcc8e33f6bdde7a95f5fcf81bfd94d70ca099d0a8bae230dbaa1"}],
    })).await;
    let event = &batch.events[0];
    assert!(event.id == "0000000006-000003-7ec94");
    assert!(event.name == "Gear.MessageEnqueued");
}

#[actix_web::test]
async fn test_gear_user_sent_messages() {
    launch_gateway();
    let client = Client::new();
    let batch = client.batch(json!({
        "gearUserMessagesSent": [{"program": "0x5466a0b28225bcc8e33f6bdde7a95f5fcf81bfd94d70ca099d0a8bae230dbaa1"}],
    })).await;
    let event = &batch.events[0];
    assert!(event.id == "0000000006-000007-7ec94");
    assert!(event.name == "Gear.UserMessageSent");
}

#[actix_web::test]
async fn test_acala_evm_executed() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "acalaEvmExecuted": [{"contract": "0x0000000000000000000100000000000000000080"}],
        }))
        .await;
    let event = &batch.events[0];
    assert!(event.id == "0001818666-000011-af202");
    assert!(event.name == "EVM.Executed");
}

#[actix_web::test]
async fn test_acala_evm_executed_wildcard_search() {
    launch_gateway();
    let client = Client::new();
    let batch = client
        .batch(json!({
            "acalaEvmExecuted": [{
                "contract": "*",
                "logs": [{
                    "contract": "0x0000000000000000000100000000000000000080",
                    "filter": [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]],
                }]
            }],
        }))
        .await;
    let event = &batch.events[0];
    assert!(event.id == "0001818666-000011-af202");
    assert!(event.name == "EVM.Executed");
}
