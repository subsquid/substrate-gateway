use crate::graphql::QueryRoot;
use crate::metrics::{HTTP_REQUESTS_TOTAL, HTTP_RESPONSE_TIME_SECONDS, HTTP_REQUESTS_ERRORS};
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use actix_web::{Result, HttpRequest, HttpResponse, App, HttpServer, HttpMessage};
use actix_web::guard::{Get, Post};
use actix_web::web::{Data, resource};
use actix_web::http::header::ContentType;
use actix_web::dev::Service;
use prometheus::{TextEncoder, Encoder};
use tracing::error;
use middleware::{Logger, BindRequestId, RequestId};

mod middleware;

async fn graphql_playground() -> Result<HttpResponse> {
    let source = playground_source(GraphQLPlaygroundConfig::new("/graphql"));
    Ok(HttpResponse::Ok().content_type("text/html; charset=utf-8").body(source))
}


async fn graphql_request(
    schema: Data<Schema<QueryRoot, EmptyMutation, EmptySubscription>>,
    req: HttpRequest,
    gql_req: GraphQLRequest
) -> GraphQLResponse {
    let extensions = req.extensions();
    let request_id = extensions.get::<RequestId>().expect("RequestId wasn't set").0.as_str();
    let response = schema.execute(gql_req.into_inner()).await;
    if response.is_err() {
        let x_squid_processor = req.headers()
            .get("X-SQUID-PROCESSOR")
            .and_then(|value| value.to_str().ok());
        for error in &response.errors {
            error!(x_squid_processor, request_id, message = error.message.as_str());
        }
        HTTP_REQUESTS_ERRORS.with_label_values(&[]).inc();
    }
    response.into()
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
            .wrap(Logger {})
            .wrap(BindRequestId {})
            .service(resource("/").guard(Get()).to(graphql_playground))
            .service(resource("/graphql").guard(Post()).to(graphql_request)
                .wrap_fn(|req, srv| {
                    HTTP_REQUESTS_TOTAL.with_label_values(&[]).inc();
                    let timer = HTTP_RESPONSE_TIME_SECONDS.with_label_values(&[])
                        .start_timer();
                    let fut = srv.call(req);
                    async move {
                        let res = fut.await?;
                        timer.observe_duration();
                        Ok(res)
                    }
                })
            )
            .service(resource("/metrics").guard(Get()).to(metrics))
    })
    .bind("0.0.0.0:8000").unwrap()
    .run()
    .await
}
