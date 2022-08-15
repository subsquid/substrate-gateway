use async_graphql::Object;
use chrono::{DateTime, Utc};
use substrate_archive::entities::{Status, Metadata, Batch, BlockHeader};

pub struct StatusObject(Status);

#[Object(name = "Status")]
impl StatusObject {
    async fn head(&self) -> i64 {
        self.0.head
    }
}

impl From<Status> for StatusObject {
    fn from(status: Status) -> Self {
        StatusObject(status)
    }
}

pub struct MetadataObject(pub Metadata);

#[Object(name = "Metadata")]
impl MetadataObject {
    async fn id(&self) -> &String {
        &self.0.id
    }

    async fn spec_name(&self) -> &String {
        &self.0.spec_name
    }

    async fn spec_version(&self) -> i64 {
        self.0.spec_version
    }

    async fn block_height(&self) -> i64 {
        self.0.block_height
    }

    async fn block_hash(&self) -> &String {
        &self.0.block_hash
    }

    async fn hex(&self) -> &String {
        &self.0.hex
    }
}

impl From<Metadata> for MetadataObject {
    fn from(metadata: Metadata) -> Self {
        MetadataObject(metadata)
    }
}

pub struct BlockHeaderObject<'a>(pub &'a BlockHeader);

#[Object(name = "BlockHeader")]
impl BlockHeaderObject<'_> {
    async fn id(&self) -> &String {
        &self.0.id
    }

    async fn height(&self) -> i64 {
        self.0.height
    }

    async fn hash(&self) -> &String {
        &self.0.hash
    }

    async fn parent_hash(&self) -> &String {
        &self.0.parent_hash
    }

    async fn state_root(&self) -> &String {
        &self.0.state_root
    }

    async fn extrinsics_root(&self) -> &String {
        &self.0.extrinsics_root
    }

    async fn timestamp(&self) -> DateTime<Utc> {
        self.0.timestamp
    }

    async fn spec_id(&self) -> &String {
        &self.0.spec_id
    }

    async fn validator(&self) -> &Option<String> {
        &self.0.validator
    }
}

pub struct BatchObject(pub Batch);

#[Object(name = "Batch")]
impl BatchObject {
    async fn header(&self) -> BlockHeaderObject<'_> {
        BlockHeaderObject(&self.0.header)
    }

    async fn calls(&self) -> &Vec<serde_json::Value> {
        &self.0.calls
    }

    async fn extrinsics(&self) -> &Vec<serde_json::Value> {
        &self.0.extrinsics
    }

    async fn events(&self) -> &Vec<serde_json::Value> {
        &self.0.events
    }
}

impl From<Batch> for BatchObject {
    fn from(batch: Batch) -> Self {
        BatchObject(batch)
    }
}
