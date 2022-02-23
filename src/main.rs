use std::env;
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql::dataloader::{DataLoader};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use actix_web::{Result, HttpResponse, App, HttpServer};
use actix_web::guard::{Get, Post};
use actix_web::web::{Data, resource};
use sqlx::postgres::PgPoolOptions;
use graphql::QueryRoot;
use graphql::loader::{ExtrinsicLoader, CallLoader, EventLoader};

mod entities;
mod graphql;
mod repository;


async fn graphql_playground() -> Result<HttpResponse> {
    let source = playground_source(GraphQLPlaygroundConfig::new("/graphql"));
    Ok(HttpResponse::Ok().content_type("text/html; charset=utf-8").body(source))
}


async fn graphql_request(
    schema: Data<Schema<QueryRoot, EmptyMutation, EmptySubscription>>,
    request: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(request.into_inner()).await.into()
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL env variable is required");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    let extrinsic_loader = DataLoader::new(ExtrinsicLoader {pool: pool.clone()}, actix_web::rt::spawn);
    let call_loader = DataLoader::new(CallLoader {pool: pool.clone()}, actix_web::rt::spawn);
    let event_loader = DataLoader::new(EventLoader {pool: pool.clone()}, actix_web::rt::spawn);
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .data(extrinsic_loader)
        .data(call_loader)
        .data(event_loader)
        .finish();
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(schema.clone()))
            .service(resource("/").guard(Get()).to(graphql_playground))
            .service(resource("/graphql").guard(Post()).to(graphql_request))
    })
    .bind("0.0.0.0:8000").unwrap()
    .run()
    .await
}
