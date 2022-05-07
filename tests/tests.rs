use std::env;
use sqlx::postgres::PgPoolOptions;
use archive_gateway::ArchiveGateway;
use serde_json::{json, Value};

#[actix_web::test]
async fn test_parent_call_loaded() {
    let database_url = env::var("TEST_DATABASE_URL").unwrap();
    let pool = PgPoolOptions::new()
        .connect(&database_url)
        .await
        .unwrap();
    let gateway = ArchiveGateway::new(pool, false);
    let handle = actix_web::rt::spawn(async move {
        gateway.run().await
    });
    let client = reqwest::Client::new();
    actix_web::rt::time::sleep(std::time::Duration::from_secs(1)).await;
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
    handle.abort();
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
    let database_url = env::var("TEST_DATABASE_URL").unwrap();
    let pool = PgPoolOptions::new()
        .connect(&database_url)
        .await
        .unwrap();
    let gateway = ArchiveGateway::new(pool, false);
    let handle = actix_web::rt::spawn(async move {
        gateway.run().await
    });
    let client = reqwest::Client::new();
    actix_web::rt::time::sleep(std::time::Duration::from_secs(1)).await;
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
    handle.abort();
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
