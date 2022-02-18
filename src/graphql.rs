use crate::entities::{Block, BlockHeader, Extrinsic, Call, Event};
use crate::repository::{get_blocks, get_extrinsics, get_calls, get_events};
use std::collections::HashMap;
use async_graphql::{Context, Object, Result, FieldError};
use async_graphql::dataloader::{Loader, DataLoader};
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
}


#[async_trait::async_trait]
impl Loader<String> for CallLoader {
    type Value = Vec<Call>;
    type Error = FieldError;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>, Self::Error> {
        let calls = get_calls(&self.pool, keys).await?;
        let mut map = HashMap::new();
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


#[Object]
impl Block {
    async fn header(&self, _ctx: &Context<'_>) -> &BlockHeader {
        &self.header
    }

    async fn extrinsics(&self, ctx: &Context<'_>) -> Result<Vec<Extrinsic>> {
        let loader = ctx.data::<DataLoader<ExtrinsicLoader>>()?;
        let extrinsics = loader.load_one(self.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new);
        Ok(extrinsics)
    }

    async fn calls(&self, ctx: &Context<'_>) -> Result<Vec<Call>> {
        let loader = ctx.data::<DataLoader<CallLoader>>()?;
        let calls = loader.load_one(self.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new);
        Ok(calls)
    }

    async fn events(&self, ctx: &Context<'_>) -> Result<Vec<Event>> {
        let loader = ctx.data::<DataLoader<EventLoader>>()?;
        let events = loader.load_one(self.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new);
        Ok(events)
    }
}


pub struct QueryRoot;


#[Object]
impl QueryRoot {
    async fn blocks(&self, ctx: &Context<'_>, limit: i32) -> Result<Vec<Block>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        Ok(get_blocks(pool, limit).await?)
    }
}
