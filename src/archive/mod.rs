use crate::entities::{Batch, Metadata, Status};
use crate::error::Error;
use selection::{EventSelection, CallSelection};

pub mod selection;
pub mod cockroach;

#[async_trait::async_trait]
pub trait ArchiveService {
    async fn batch(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        event_selections: &Vec<EventSelection>,
        call_selections: &Vec<CallSelection>,
        include_all_blocks: bool
    ) -> Result<Vec<Batch>, Error>;
    async fn metadata(&self) -> Result<Vec<Metadata>, Error>;
    async fn status(&self) -> Result<Status, Error>;
}
