mod graphql;
mod entities;

use sqlx::postgres::{PgPoolOptions};
use graphql::QueryRoot;
use rocket::{response::content, State, routes};
use async_graphql::{
    http::{playground_source, GraphQLPlaygroundConfig},
    Schema, EmptyMutation, EmptySubscription
};
use async_graphql_rocket::{GraphQLRequest, GraphQLResponse};


#[rocket::get("/")]
fn graphql_playground() -> content::Html<String> {
    content::Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
}


#[rocket::post("/graphql", data = "<request>", format = "application/json")]
async fn graphql_request(
    schema: &State<Schema<QueryRoot, EmptyMutation, EmptySubscription>>,
    request: GraphQLRequest
) -> GraphQLResponse {
    request.execute(schema).await
}


#[rocket::main]
async fn main() {
    let database_url = "postgresql://postgres:postgres@localhost:29387/archive";
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url).await.unwrap();
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish();
    rocket::build()
        .manage(schema)
        .mount(
            "/",
            routes![
                graphql_playground,
                graphql_request,
            ],
        )
        .launch()
        .await
        .unwrap();
}
