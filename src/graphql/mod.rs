use crate::entities::{Block, BlockHeader, Extrinsic, Call, Event, Metadata, Status};
use crate::repository::{get_blocks, get_metadata, get_status, EventSelection, CallSelection};
use std::sync::Arc;
use sqlx::{Pool, Postgres};
use async_graphql::{Context, Object, Result};
use async_graphql::dataloader::DataLoader;
use loader::{ExtrinsicLoader, CallLoader, EventLoader};


pub mod loader;


struct BlockContext {
    call_loader: Option<DataLoader<CallLoader>>,
}


impl BlockContext {
    fn new(call_loader: Option<DataLoader<CallLoader>>) -> Self {
        Self {
            call_loader,
        }
    }
}


struct BlockObject {
    block: Block,
    context: Arc<BlockContext>,
}


impl BlockObject {
    pub fn new(block: Block, context: Arc<BlockContext>) -> Self {
        Self {
            block,
            context
        }
    }
}


#[Object(name = "Block")]
impl BlockObject {
    async fn header(&self, _ctx: &Context<'_>) -> &BlockHeader {
        &self.block.header
    }

    async fn extrinsics(&self, ctx: &Context<'_>) -> Result<Vec<Extrinsic>> {
        let loader = ctx.data::<DataLoader<ExtrinsicLoader>>()?;
        let extrinsics = loader.load_one(self.block.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new);
        Ok(extrinsics)
    }

    async fn calls(&self, _ctx: &Context<'_>) -> Result<Vec<Call>> {
        let calls = self.context.call_loader
            .as_ref()
            .unwrap()
            .load_one(self.block.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new);
        Ok(calls)
    }

    async fn events(&self, ctx: &Context<'_>) -> Result<Vec<Event>> {
        let loader = ctx.data::<DataLoader<EventLoader>>()?;
        let events = loader.load_one(self.block.header.id.clone())
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
    ) -> Result<Vec<BlockObject>> {
        let pool = ctx.data::<Pool<Postgres>>()?;
        let call_loader = calls.as_ref().and_then(|calls| {
            let loader = CallLoader::new(pool.clone(), calls.clone());
            Some(DataLoader::new(loader, actix_web::rt::spawn))
        });
        let block_context = Arc::new(BlockContext::new(call_loader));
        let blocks = get_blocks(pool, limit, from_block, to_block, events, &calls, include_all_blocks)
            .await?
            .into_iter()
            .map(|block| BlockObject::new(block, block_context.clone()))
            .collect();
        Ok(blocks)
    }

    async fn metadata(&self, ctx: &Context<'_>) -> Result<Vec<Metadata>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        Ok(get_metadata(&pool).await?)
    }

    async fn status(&self, ctx: &Context<'_>) -> Result<Status> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        Ok(get_status(&pool).await?)
    }
}
