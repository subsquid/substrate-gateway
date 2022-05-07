use serde_json::json;
use common::{launch_gateway, GatewayResponse, BatchResponse};

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
    let gateway_response = response.json::<GatewayResponse<BatchResponse>>().await.unwrap();
    let batch = gateway_response.data.batch;
    let requested_call = batch[0].calls.iter()
        .find(|call| call.id == "0000650677-000003-0f08a-000001".to_string());
    let parent_call = batch[0].calls.iter()
        .find(|call| call.id == "0000650677-000003-0f08a".to_string());
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
    let gateway_response = response.json::<GatewayResponse<BatchResponse>>().await.unwrap();
    let batch = gateway_response.data.batch;
    let requested_call = batch[0].calls.iter()
        .find(|call| call.id == "0000650677-000003-0f08a-000001".to_string());
    let parent_call = batch[0].calls.iter()
        .find(|call| call.id == "0000650677-000003-0f08a".to_string());
    assert!(requested_call.is_some());
    assert!(parent_call.is_none());
}
