use crate::graphql::QueryRoot;
use crate::metrics::{HTTP_REQUESTS_TOTAL, HTTP_RESPONSE_TIME_SECONDS};
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use actix_web::{Result, HttpResponse, App, HttpServer};
use actix_web::guard::{Get, Post};
use actix_web::web::{Data, resource};
use actix_web::middleware::Logger;
use actix_web::http::header::ContentType;
use actix_web::dev::Service;
use prometheus::{TextEncoder, Encoder};


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


async fn metrics() -> Result<HttpResponse, actix_web::Error> {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    encoder.encode(&prometheus::gather(), &mut buffer).expect("Failed to encode metrics");
    let response = String::from_utf8(buffer.clone()).expect("Failed to convert bytes to string");
    buffer.clear();
    Ok(HttpResponse::Ok()
        .insert_header(ContentType(mime::TEXT_PLAIN))
        .body(response))
}


pub async fn run(schema: Schema<QueryRoot, EmptyMutation, EmptySubscription>) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(schema.clone()))
            .wrap(Logger::default())
            .service(resource("/").guard(Get()).to(graphql_playground))
            .service(resource("/graphql").guard(Post()).to(graphql_request).wrap_fn(|req, srv| {
                HTTP_REQUESTS_TOTAL.with_label_values(&[]).inc();
                let timer = HTTP_RESPONSE_TIME_SECONDS.with_label_values(&[]).start_timer();

                let fut = srv.call(req);
                async {
                    let res = fut.await?;
                    timer.observe_duration();
                    Ok(res)
                }
            }))
            .service(resource("/metrics").guard(Get()).to(metrics))
    })
    .bind("0.0.0.0:8000").unwrap()
    .run()
    .await
}