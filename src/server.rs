use crate::graphql::QueryRoot;
use crate::metrics::{HTTP_REQUESTS_TOTAL, HTTP_RESPONSE_TIME_SECONDS, SERIALIZATION_TIME_SPENT_SECONDS, HTTP_REQUESTS_ERRORS};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use actix_web::{Result, HttpRequest, HttpResponse, App, HttpServer, HttpMessage};
use actix_web::guard::{Get, Post};
use actix_web::web::{Data, resource};
use actix_web::middleware::Logger;
use actix_web::http::header::ContentType;
use actix_web::dev::Service;
use prometheus::{TextEncoder, Encoder};



/// Timer to measure the time spent to waiting result from database
#[derive(Debug)]
pub struct DbTimer {
    intervals: Vec<(Duration, Duration)>,
}


impl DbTimer {
    pub fn new() -> Self {
        DbTimer {
            intervals: Vec::new(),
        }
    }

    pub fn spent_time(&mut self) -> Duration {
        let mut spent_time = Duration::new(0, 0);
        self.intervals.sort_by(|first_interval, second_interval| {
            first_interval.0.partial_cmp(&second_interval.0).unwrap()
        });
        let mut temp_interval: Option<(Duration, Duration)> = None;
        for interval in &self.intervals {
            if let Some(mut temp_value) = temp_interval {
                if interval.0 > temp_value.1 {
                    spent_time = spent_time.saturating_add(temp_value.1 - temp_value.0);
                    temp_interval = None;
                } else {
                    if interval.1 > temp_value.1 {
                        temp_value.1 = interval.1;
                    }
                }
            } else {
                temp_interval = Some(interval.clone());
            }
        }
        if let Some(temp_value) = temp_interval {
            spent_time = spent_time.saturating_add(temp_value.1 - temp_value.0);
        }
        spent_time
    }

    pub fn add_interval(&mut self, interval: (Duration, Duration)) {
        self.intervals.push(interval);
    }
}


#[inline]
pub fn duration_to_seconds(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos()) / 1e9;
    d.as_secs() as f64 + nanos
}


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
    let db_timer = extensions.get::<Arc<Mutex<DbTimer>>>().expect("DbTimer wasn't initialized");
    let request = gql_req.into_inner().data(db_timer.clone());
    let response = schema.execute(request).await;
    if response.is_err() {
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
            .wrap(Logger::default())
            .service(resource("/").guard(Get()).to(graphql_playground))
            .service(resource("/graphql").guard(Post()).to(graphql_request).wrap_fn(|req, srv| {
                HTTP_REQUESTS_TOTAL.with_label_values(&[]).inc();
                let request_timer = HTTP_RESPONSE_TIME_SECONDS.with_label_values(&[]).start_timer();
                let db_timer = Arc::new(Mutex::new(DbTimer::new()));
                req.extensions_mut().insert(db_timer.clone());

                let fut = srv.call(req);
                async move {
                    let res = fut.await?;
                    let request_time = request_timer.stop_and_record();
                    let serialization_timer = SERIALIZATION_TIME_SPENT_SECONDS.with_label_values(&[]);
                    let db_time = duration_to_seconds(db_timer.lock().unwrap().spent_time());
                    let serialization_time = request_time - db_time;
                    serialization_timer.observe(serialization_time);
                    Ok(res)
                }
            }))
            .service(resource("/metrics").guard(Get()).to(metrics))
    })
    .bind("0.0.0.0:8000").unwrap()
    .run()
    .await
}
