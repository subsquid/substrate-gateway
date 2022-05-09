use std::{env, thread};
use std::sync::Once;
use std::time::Duration;
use sqlx::postgres::PgPoolOptions;
use actix_web::rt::{Runtime, spawn};
use actix_web::rt::time::sleep;
use archive_gateway::{ArchiveGateway, DatabaseType};
use serde::Deserialize;
use serde_json::Value;

static INIT: Once = Once::new();

pub fn launch_gateway() {
    INIT.call_once(|| {
        let handle = thread::spawn(|| {
            Runtime::new()
                .unwrap()
                .block_on(async {
                    let database_url = env::var("TEST_DATABASE_URL").unwrap();
                    let pool = PgPoolOptions::new()
                        .connect(&database_url)
                        .await
                        .unwrap();
                    let database_type = match env::var("DATABASE_TYPE")
                        .expect("DATABASE_TYPE env variable is required")
                        .as_str() {
                            "postgres" => DatabaseType::Postgres,
                            "cockroach" => DatabaseType::Cockroach,
                            _ => panic!("DATABASE_TYPE env should be `postgres` or `cockroach`")
                        };
                    spawn(async {
                        ArchiveGateway::new(pool, database_type, false).run().await
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
    pub success: Option<bool>,
    pub name: Option<String>,
    pub args: Option<Value>,
    pub pos: i32,
}

#[derive(Deserialize)]
pub struct Batch {
    pub calls: Vec<Call>,
}

#[derive(Deserialize)]
pub struct BatchResponse {
    pub batch: Vec<Batch>,
}

#[derive(Deserialize)]
pub struct GatewayResponse<T> {
    pub data: T,
}