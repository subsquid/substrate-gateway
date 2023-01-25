use super::{batch::BatchLoader, BatchResponse};
use crate::entities::Batch;
use crate::error::Error;
use crate::Selections;
use std::cmp::{max, min};
use std::time::{Duration, Instant};
use tracing::debug;

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
    scan_start_value: u16,
    scan_max_value: u32,
}

impl PartialBatchLoader {
    pub fn new(
        loader: BatchLoader,
        scan_start_value: u16,
        scan_max_value: u32,
    ) -> PartialBatchLoader {
        PartialBatchLoader {
            loader,
            scan_start_value,
            scan_max_value,
        }
    }

    pub async fn load(&self, options: &PartialOptions) -> Result<BatchResponse, Error> {
        let mut batch = vec![];

        let start_time = Instant::now();
        let timeout = Duration::from_secs(5);
        let scan_start_value: i32 = self.scan_start_value.into();
        let scan_max_value: i32 = self.scan_max_value.try_into().unwrap();
        let mut size = 0;

        let mut from_block = options.from_block;
        let mut range_width = scan_start_value;
        let mut to_block = min(from_block + range_width - 1, options.to_block);
        let mut total_range = 0;

        loop {
            debug!("scanning from {from_block} to {to_block}");
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
                min(range_width * 10, scan_max_value)
            } else {
                let total_blocks = i32::try_from(batch.len()).unwrap();
                min(
                    max(
                        (total_range / total_blocks) * (scan_start_value - len),
                        scan_start_value,
                    ),
                    scan_max_value,
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
