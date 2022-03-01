use crate::entities::{Block, BlockHeader, Extrinsic, Call, Event, Metadata, Status};
use crate::repository::{get_blocks, get_metadata, get_status, EventSelection, CallSelection};
use std::sync::Arc;
use sqlx::{Pool, Postgres};
use async_graphql::{Context, Object, Result};
use async_graphql::dataloader::DataLoader;
use loader::{ExtrinsicLoader, CallLoader, EventLoader};


pub mod loader;


struct BlockContext {
    call_loader: DataLoader<CallLoader>,
    extrinsic_loader: DataLoader<ExtrinsicLoader>,
    event_loader: DataLoader<EventLoader>,
}


impl BlockContext {
    fn new(
        call_loader: DataLoader<CallLoader>,
        extrinsic_loader: DataLoader<ExtrinsicLoader>,
        event_loader: DataLoader<EventLoader>
    ) -> Self {
        Self {
            call_loader,
            extrinsic_loader,
            event_loader,
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

    async fn extrinsics(&self, _ctx: &Context<'_>) -> Result<Vec<Extrinsic>> {
        let extrinsics = self.context.extrinsic_loader
            .load_one(self.block.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new);
        Ok(extrinsics)
    }

    async fn calls(&self, _ctx: &Context<'_>) -> Result<Vec<Call>> {
        let calls = self.context.call_loader
            .load_one(self.block.header.id.clone())
            .await?
            .unwrap_or_else(Vec::new);
        Ok(calls)
    }

    async fn events(&self, _ctx: &Context<'_>) -> Result<Vec<Event>> {
        let events = self.context.event_loader
            .load_one(self.block.header.id.clone())
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
        let call_loader = DataLoader::new(
            CallLoader::new(pool.clone(), calls.clone(), events.clone()),
            actix_web::rt::spawn
        );
        let extrinsic_loader = DataLoader::new(
            ExtrinsicLoader::new(pool.clone(), calls.clone(), events.clone()),
            actix_web::rt::spawn
        );
        let event_loader = DataLoader::new(
            EventLoader::new(pool.clone(), events.clone()),
            actix_web::rt::spawn
        );
        let block_context = Arc::new(BlockContext::new(call_loader, extrinsic_loader, event_loader));
        let blocks = get_blocks(pool, limit, from_block, to_block, events, calls, include_all_blocks)
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
