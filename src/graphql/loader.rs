use crate::entities::{Extrinsic, Call, Event};
use crate::repository::{get_extrinsics, get_calls, get_events, CallSelection};
use std::collections::HashMap;
use async_graphql::FieldError;
use async_graphql::dataloader::Loader;
use sqlx::{Pool, Postgres};


pub struct ExtrinsicLoader {
    pub pool: Pool<Postgres>,
}

#[async_trait::async_trait]
impl Loader<String> for ExtrinsicLoader {
    type Value = Vec<Extrinsic>;
    type Error = FieldError;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>, Self::Error> {
        let extrinsics = get_extrinsics(&self.pool, keys).await?;
        let mut map = HashMap::new();
        for extrinsic in extrinsics {
            let block_extrinsics = map.entry(extrinsic.block_id.clone()).or_insert_with(Vec::new);
            block_extrinsics.push(extrinsic);
        }
        Ok(map)
    }
}


pub struct CallLoader {
    pub pool: Pool<Postgres>,
    pub selections: Vec<CallSelection>,
}


impl CallLoader {
    pub fn new(pool: Pool<Postgres>, selections: Vec<CallSelection>) -> Self {
        Self {
            pool,
            selections,
        }
    }
}


#[async_trait::async_trait]
impl Loader<String> for CallLoader {
    type Value = Vec<Call>;
    type Error = FieldError;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>, Self::Error> {
        let mut map = HashMap::new();
        let calls = get_calls(&self.pool, keys, &self.selections).await?;
        for call in calls {
            let block_calls = map.entry(call.block_id.clone()).or_insert_with(Vec::new);
            block_calls.push(call);
        }
        Ok(map)
    }
}


pub struct EventLoader {
    pub pool: Pool<Postgres>,
}


#[async_trait::async_trait]
impl Loader<String> for EventLoader {
    type Value = Vec<Event>;
    type Error = FieldError;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>, Self::Error> {
        let events = get_events(&self.pool, keys).await?;
        let mut map = HashMap::new();
        for event in events {
            let block_events = map.entry(event.block_id.clone()).or_insert_with(Vec::new);
            block_events.push(event);
        }
        Ok(map)
    }
}
