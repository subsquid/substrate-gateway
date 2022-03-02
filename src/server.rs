use crate::graphql::QueryRoot;
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use actix_web::{Result, HttpResponse, App, HttpServer};
use actix_web::guard::{Get, Post};
use actix_web::web::{Data, resource};
use actix_web::middleware::Logger;


async fn graphql_playground() -> Result<HttpResponse> {
    let source = playground_source(GraphQLPlaygroundConfig::new("/graphql"));
    Ok(HttpResponse::Ok().content_type("text/html; charset=utf-8").body(source))
}


async fn graphql_request(
    schema: Data<Schema<QueryRoot, EmptyMutation, EmptySubscription>>,
    request: GraphQLRequest
) -> GraphQLResponse {
    schema.execute(request.into_inner()).await.into()
}


pub async fn run(schema: Schema<QueryRoot, EmptyMutation, EmptySubscription>) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(schema.clone()))
            .wrap(Logger::default())
            .service(resource("/").guard(Get()).to(graphql_playground))
            .service(resource("/graphql").guard(Post()).to(graphql_request))
    })
    .bind("0.0.0.0:8000").unwrap()
    .run()
    .await
}