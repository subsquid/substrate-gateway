use crate::entities::{Block, BlockHeader, Extrinsic, Call, Event};
use crate::repository::{get_blocks, EventSelection, CallSelection};
use async_graphql::{Context, Object, Result};
use async_graphql::dataloader::DataLoader;
use loader::{ExtrinsicLoader, CallLoader, EventLoader};


pub mod loader;


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
    async fn blocks(
        &self,
        ctx: &Context<'_>,
        limit: i32,
        #[graphql(default = 0)] from_block: i32,
        to_block: Option<i32>,
        events: Option<Vec<EventSelection>>,
        calls: Option<Vec<CallSelection>>,
        include_all_blocks: Option<bool>,
    ) -> Result<Vec<Block>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        Ok(get_blocks(pool, limit, from_block, to_block, events, calls, include_all_blocks).await?)
    }
}
