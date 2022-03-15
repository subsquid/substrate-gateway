use crate::entities::{Extrinsic, Call, Event};
use crate::repository::{get_extrinsics, get_calls, get_events, CallSelection, EventSelection};
use crate::metrics::DB_TIME_SPENT_SECONDS;
use crate::server::DbTimer;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use async_graphql::FieldError;
use async_graphql::dataloader::Loader;
use sqlx::{Pool, Postgres};


pub struct ExtrinsicLoader {
    pool: Pool<Postgres>,
    call_selections: Option<Vec<CallSelection>>,
    event_selections: Option<Vec<EventSelection>>,
    db_timer: Arc<Mutex<DbTimer>>
}


impl ExtrinsicLoader {
    pub fn new(
        pool: Pool<Postgres>,
        call_selections: Option<Vec<CallSelection>>,
        event_selections: Option<Vec<EventSelection>>,
        db_timer: Arc<Mutex<DbTimer>>
    ) -> Self {
        Self {
            pool,
            call_selections,
            event_selections,
            db_timer,
        }
    }
}


#[async_trait::async_trait]
impl Loader<String> for ExtrinsicLoader {
    type Value = Vec<Extrinsic>;
    type Error = FieldError;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>, Self::Error> {
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["extrinsic"]).start_timer();
        let query_start = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let extrinsics = get_extrinsics(&self.pool, keys, &self.call_selections, &self.event_selections).await?;
        let query_finish = SystemTime::now().duration_since(UNIX_EPOCH)?;
        self.db_timer.lock().unwrap().add_interval((query_start, query_finish));
        timer.observe_duration();
        let mut map = HashMap::new();
        for extrinsic in extrinsics {
            let block_extrinsics = map.entry(extrinsic._block_id.clone()).or_insert_with(Vec::new);
            block_extrinsics.push(extrinsic);
        }
        Ok(map)
    }
}


pub struct CallLoader {
    pool: Pool<Postgres>,
    call_selections: Option<Vec<CallSelection>>,
    event_selections: Option<Vec<EventSelection>>,
    db_timer: Arc<Mutex<DbTimer>>,
}


impl CallLoader {
    pub fn new(
        pool: Pool<Postgres>,
        call_selections: Option<Vec<CallSelection>>,
        event_selections: Option<Vec<EventSelection>>,
        db_timer: Arc<Mutex<DbTimer>>,
    ) -> Self {
        Self {
            pool,
            call_selections,
            event_selections,
            db_timer,
        }
    }
}


#[async_trait::async_trait]
impl Loader<String> for CallLoader {
    type Value = Vec<Call>;
    type Error = FieldError;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>, Self::Error> {
        let mut map = HashMap::new();
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["call"]).start_timer();
        let query_start = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let calls = get_calls(&self.pool, keys, &self.call_selections, &self.event_selections).await?;
        let query_finish = SystemTime::now().duration_since(UNIX_EPOCH)?;
        self.db_timer.lock().unwrap().add_interval((query_start, query_finish));
        timer.observe_duration();
        for call in calls {
            let block_calls = map.entry(call._block_id.clone()).or_insert_with(Vec::new);
            block_calls.push(call);
        }
        Ok(map)
    }
}


pub struct EventLoader {
    pool: Pool<Postgres>,
    event_selections: Option<Vec<EventSelection>>,
    db_timer: Arc<Mutex<DbTimer>>
}


impl EventLoader {
    pub fn new(
        pool: Pool<Postgres>,
        event_selections: Option<Vec<EventSelection>>,
        db_timer: Arc<Mutex<DbTimer>>
    ) -> Self {
        Self {
            pool,
            event_selections,
            db_timer
        }
    }
}


#[async_trait::async_trait]
impl Loader<String> for EventLoader {
    type Value = Vec<Event>;
    type Error = FieldError;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>, Self::Error> {
        let timer = DB_TIME_SPENT_SECONDS.with_label_values(&["event"]).start_timer();
        let query_start = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let events = get_events(&self.pool, keys, &self.event_selections).await?;
        let query_finish = SystemTime::now().duration_since(UNIX_EPOCH)?;
        self.db_timer.lock().unwrap().add_interval((query_start, query_finish));
        timer.observe_duration();
        let mut map = HashMap::new();
        for event in events {
            let block_events = map.entry(event._block_id.clone()).or_insert_with(Vec::new);
            block_events.push(event);
        }
        Ok(map)
    }
}
