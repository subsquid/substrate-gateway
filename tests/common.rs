use actix_web::rt::time::sleep;
use actix_web::rt::{spawn, Runtime};
use serde::Deserialize;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use std::sync::Once;
use std::time::Duration;
use std::{env, thread};
use substrate_gateway::SubstrateGateway;

static INIT: Once = Once::new();

pub fn launch_gateway() {
    INIT.call_once(|| {
        let handle = thread::spawn(|| {
            Runtime::new().unwrap().block_on(async {
                let database_url = env::var("TEST_DATABASE_URL").unwrap();
                let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
                spawn(async {
                    SubstrateGateway::new(pool)
                        .evm_support(true)
                        .contracts_support(true)
                        .gear_support(true)
                        .acala_support(true)
                        .run()
                        .await
                });
                sleep(Duration::from_secs(1)).await;
            });
        });
        handle.join().unwrap();
    })
}

#[derive(Deserialize)]
pub struct Call {
    pub id: String,
    pub parent_id: Option<String>,
    pub block_id: Option<String>,
    pub extrinsic_id: Option<String>,
    pub success: bool,
    pub name: String,
    pub args: Option<Value>,
    pub pos: i32,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
pub struct Event {
    pub id: String,
    pub block_id: Option<String>,
    pub index_in_block: Option<i32>,
    pub phase: Option<String>,
    pub extrinsic_id: Option<String>,
    pub call_id: Option<String>,
    pub name: String,
    pub args: Option<Value>,
    pub pos: i32,
    pub evmTxHash: Option<String>,
}

#[derive(Deserialize)]
pub struct Batch {
    pub calls: Vec<Call>,
    pub events: Vec<Event>,
}

#[derive(Deserialize)]
pub struct BatchResponse {
    pub batch: Vec<Batch>,
}

#[derive(Deserialize)]
pub struct GatewayResponse<T> {
    pub data: T,
}

fn args_to_string(args: &Value, root: bool) -> String {
    if args.is_array() {
        let list = args
            .as_array()
            .unwrap()
            .iter()
            .map(|value| args_to_string(value, false))
            .collect::<Vec<String>>()
            .join(", ");
        format!("[{}]", list)
    } else if args.is_object() {
        let object = args
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| format!("{}: {}", key, args_to_string(value, false)))
            .collect::<Vec<String>>()
            .join(", ");
        if root {
            object
        } else {
            format!("{{{}}}", object)
        }
    } else {
        format!("{}", args)
    }
}

pub struct Client(reqwest::Client);

impl Client {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Client(reqwest::Client::new())
    }

    pub async fn batch(&self, args: Value) -> Batch {
        let json = serde_json::json!({
            "query": format!("{{ batch({}) {{ calls, events, extrinsics }} }}", args_to_string(&args, true)),
        });
        let response = self
            .0
            .post("http://0.0.0.0:8000/graphql")
            .json(&json)
            .send()
            .await
            .unwrap();
        let text = response.text().await.unwrap();
        match serde_json::from_str::<GatewayResponse<BatchResponse>>(&text) {
            Ok(mut response) => response.data.batch.remove(0),
            Err(_) => panic!("Unexpected response body: {}", text),
        }
    }
}
