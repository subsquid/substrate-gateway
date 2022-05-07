use serde_json::{json, Value};
use common::launch_gateway;

mod common;

#[actix_web::test]
async fn test_parent_call_loaded() {
    launch_gateway();
    let client = reqwest::Client::new();
    let response = client.post("http://0.0.0.0:8000/graphql")
        .json(&json!({"query": "{ batch(limit: 1, calls: [{name: \"Balances.transfer\", data: {call: {parent: {_all: true}}}}]) { calls } }"}))
        .send()
        .await
        .unwrap();
    let data = response
        .json::<Value>()
        .await
        .unwrap();
    let calls = data
        .get("data")
        .unwrap()
        .get("batch")
        .unwrap()
        .get(0)
        .unwrap()
        .get("calls")
        .unwrap()
        .as_array()
        .unwrap();
    let requested_call = calls.iter()
        .find(|call| {
            let id = call.get("id").unwrap().as_str().unwrap();
            id == "0000650677-000003-0f08a-000001".to_string()
        });
    let parent_call = calls.iter()
        .find(|call| {
            let id = call.get("id").unwrap().as_str().unwrap();
            id == "0000650677-000003-0f08a".to_string()
        });
    assert!(requested_call.is_some());
    assert!(parent_call.is_some());
}

#[actix_web::test]
async fn test_parent_call_skipped() {
    launch_gateway();
    let client = reqwest::Client::new();
    let response = client.post("http://0.0.0.0:8000/graphql")
        .json(&json!({"query": "{ batch(limit: 1, calls: [{name: \"Balances.transfer\", data: {call: {parent: {_all: false}}}}]) { calls } }"}))
        .send()
        .await
        .unwrap();
    let data = response
        .json::<Value>()
        .await
        .unwrap();
    let calls = data
        .get("data")
        .unwrap()
        .get("batch")
        .unwrap()
        .get(0)
        .unwrap()
        .get("calls")
        .unwrap()
        .as_array()
        .unwrap();
    let requested_call = calls.iter()
        .find(|call| {
            let id = call.get("id").unwrap().as_str().unwrap();
            id == "0000650677-000003-0f08a-000001".to_string()
        });
    let parent_call = calls.iter()
        .find(|call| {
            let id = call.get("id").unwrap().as_str().unwrap();
            id == "0000650677-000003-0f08a".to_string()
        });
    assert!(requested_call.is_some());
    assert!(parent_call.is_none());
}
