use serde_json::json;
use common::{launch_gateway, Client};

mod common;

#[actix_web::test]
async fn test_parent_call_loaded() {
    launch_gateway();
    let client = Client::new();
    let batch = client.batch(json!({
        "limit": 1,
        "calls": [{
            "name": "Balances.transfer",
            "data": {
                "call": {
                    "parent": {"_all": true}
                }
            }
        }]
    })).await;
    let requested_call = batch.calls.iter()
        .find(|call| call.id == "0000650677-000003-0f08a-000001".to_string());
    let parent_call = batch.calls.iter()
        .find(|call| call.id == "0000650677-000003-0f08a".to_string());
    assert!(requested_call.is_some());
    assert!(parent_call.is_some());
}

#[actix_web::test]
async fn test_parent_call_skipped() {
    launch_gateway();
    let client = Client::new();
    let batch = client.batch(json!({
        "limit": 1,
        "calls": [{
            "name": "Balances.transfer",
            "data": {
                "call": {
                    "parent": {"_all": false}
                }
            }
        }]
    })).await;
    let requested_call = batch.calls.iter()
        .find(|call| call.id == "0000650677-000003-0f08a-000001".to_string());
    let parent_call = batch.calls.iter()
        .find(|call| call.id == "0000650677-000003-0f08a".to_string());
    assert!(requested_call.is_some());
    assert!(parent_call.is_none());
}

#[actix_web::test]
async fn test_evm_log_has_tx_hash() {
    launch_gateway();
    let client = Client::new();
    let batch = client.batch(json!({
        "limit": 1,
        "evmLogs": [{
            "contract": "0xb654611f84a8dc429ba3cb4fda9fad236c505a1a",
            "filter": [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]],
            "data": {
                "txHash": true,
                "substrate": {
                    "event": {"name": true}
                }
            }
        }]
    })).await;
    let log = &batch.events[0];
    assert!(log.id == "0000569006-000084-5e412".to_string());
    assert!(log.txHash.clone().unwrap() == "0x8eafe131eee90e0dfb07d6df46b1aea737834936968da31f807af566a59148b9".to_string());
}
