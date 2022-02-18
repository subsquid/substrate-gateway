use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql::dataloader::{DataLoader};
use async_graphql_rocket::{GraphQLRequest, GraphQLResponse};
use rocket::{response::content, routes, State};
use sqlx::postgres::PgPoolOptions;
use graphql::QueryRoot;

mod entities;
mod graphql;
mod repository;


#[rocket::get("/")]
fn graphql_playground() -> content::Html<String> {
    content::Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
}


#[rocket::post("/graphql", data = "<request>", format = "application/json")]
async fn graphql_request(
    schema: &State<Schema<QueryRoot, EmptyMutation, EmptySubscription>>,
    request: GraphQLRequest,
) -> GraphQLResponse {
    request.execute(schema).await
}


#[rocket::main]
async fn main() {
    let database_url = "postgresql://postgres:postgres@localhost:29387/archive";
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .unwrap();

    let extrinsic_loader = DataLoader::new(graphql::ExtrinsicLoader {pool: pool.clone()}, tokio::task::spawn);
    let call_loader = DataLoader::new(graphql::CallLoader {pool: pool.clone()}, tokio::task::spawn);
    let event_loader = DataLoader::new(graphql::EventLoader {pool: pool.clone()}, tokio::task::spawn);
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .data(extrinsic_loader)
        .data(call_loader)
        .data(event_loader)
        .finish();
    rocket::build()
        .manage(schema)
        .mount("/", routes![graphql_playground, graphql_request,])
        .launch()
        .await
        .unwrap();
}
