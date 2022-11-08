use super::{batch::BatchLoader, BatchResponse};
use crate::entities::Batch;
use crate::error::Error;
use crate::Selections;
use std::time::{Instant, Duration};
use std::cmp::{max, min};

const AVERAGE_EVENT_SIZE: usize = 250;
const AVERAGE_CALL_SIZE: usize = 700;
const AVERAGE_EXTRINSIC_SIZE: usize = 350;

pub struct PartialOptions {
    pub from_block: i32,
    pub to_block: i32,
    pub include_all_blocks: bool,
    pub selections: Selections,
}

pub struct PartialBatchLoader {
    loader: BatchLoader,
}

impl PartialBatchLoader {
    pub fn new(loader: BatchLoader) -> PartialBatchLoader {
        PartialBatchLoader { loader }
    }

    pub async fn load(&self, options: &PartialOptions) -> Result<BatchResponse, Error> {
        let mut batch = vec![];

        let start_time = Instant::now();
        let timeout = Duration::from_secs(5);
        let mut size = 0;

        let mut from_block = options.from_block;
        let mut range_width = 100;
        let mut to_block = min(from_block + range_width - 1, options.to_block);
        let mut total_range = 0;

        loop {
            let mut range_batch = self
                .loader
                .load(
                    from_block,
                    to_block,
                    options.include_all_blocks,
                    &options.selections,
                )
                .await?;
            let len = i32::try_from(range_batch.len()).unwrap();
            total_range += range_width;
            size += size_of_batch(&range_batch);
            batch.append(&mut range_batch);

            if size > 1024 * 1024 {
                break;
            }

            if timeout < start_time.elapsed() {
                break;
            }

            if to_block == options.to_block {
                break;
            }

            range_width = if len == 0 {
                min(range_width * 10, 100_000)
            } else {
                let total_blocks = i32::try_from(batch.len()).unwrap();
                min(
                    max((total_range / total_blocks) * (100 - len), 100),
                    100_000,
                )
            };

            from_block = to_block + 1;
            to_block = min(from_block + range_width - 1, options.to_block);
        }

        Ok(BatchResponse {
            data: batch,
            next_block: Some(to_block + 1),
        })
    }
}

fn size_of_batch(batch: &Vec<Batch>) -> usize {
    let mut size = 0;
    for item in batch {
        size += item.calls.len() * AVERAGE_CALL_SIZE;
        size += item.events.len() * AVERAGE_EVENT_SIZE;
        size += item.extrinsics.len() * AVERAGE_EXTRINSIC_SIZE;
    }
    size
}
