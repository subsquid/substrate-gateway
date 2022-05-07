use std::{env, thread};
use std::sync::Once;
use std::time::Duration;
use sqlx::postgres::PgPoolOptions;
use actix_web::rt::{Runtime, spawn};
use actix_web::rt::time::sleep;
use archive_gateway::ArchiveGateway;

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
                    spawn(async {
                        ArchiveGateway::new(pool, false).run().await
                    });
                    sleep(Duration::from_secs(1)).await;
                });
        });
        handle.join().unwrap();
    })
}
