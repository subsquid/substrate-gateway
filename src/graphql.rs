use super::entities::Block;
use async_graphql::{FieldResult, Object, Context};


pub struct QueryRoot;


#[Object]
impl QueryRoot{
    async fn block(&self, ctx: &Context<'_>, limit: Option<i32>) -> FieldResult<Vec<Block>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>().unwrap();
        let mut query = String::from("SELECT * FROM block");
        if let Some(limit) = limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }
        let result = sqlx::query_as::<_, Block>(&query)
            .fetch_all(pool)
            .await?;
        Ok(result)
    }
}
