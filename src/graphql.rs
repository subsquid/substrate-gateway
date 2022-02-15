use super::entities::{Block, Extrinsic, Call, Event};
use async_graphql::{FieldResult, Object, Context};


pub struct QueryRoot;


#[Object]
impl QueryRoot{
    async fn block(&self, ctx: &Context<'_>, limit: Option<i32>) -> FieldResult<Vec<Block>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let mut query = String::from("SELECT * FROM block");
        if let Some(limit) = limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }
        let result = sqlx::query_as::<_, Block>(&query)
            .fetch_all(pool)
            .await?;
        Ok(result)
    }

    async fn extrinsic(&self, ctx: &Context<'_>, limit: Option<i32>) -> FieldResult<Vec<Extrinsic>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let mut query = String::from("SELECT * FROM extrinsic");
        if let Some(limit) = limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }
        let result = sqlx::query_as::<_, Extrinsic>(&query)
            .fetch_all(pool)
            .await?;
        Ok(result)
    }

    async fn call(&self, ctx: &Context<'_>, limit: Option<i32>) -> FieldResult<Vec<Call>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let mut query = String::from("SELECT * FROM call");
        if let Some(limit) = limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }
        let result = sqlx::query_as::<_, Call>(&query)
            .fetch_all(pool)
            .await?;
        Ok(result)
    }

    async fn event(&self, ctx: &Context<'_>, limit: Option<i32>) -> FieldResult<Vec<Event>> {
        let pool = ctx.data::<sqlx::Pool<sqlx::Postgres>>()?;
        let mut query = String::from("SELECT * FROM event");
        if let Some(limit) = limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }
        let result = sqlx::query_as::<_, Event>(&query)
            .fetch_all(pool)
            .await?;
        Ok(result)
    }
}
