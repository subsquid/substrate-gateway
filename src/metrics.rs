use lazy_static::lazy_static;
use prometheus::{IntCounterVec, HistogramVec, register_int_counter_vec, register_histogram_vec, opts};

const HTTP_RESPONSE_TIME_BUCKETS: &[f64; 8] = &[
    0.1, 0.2, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0
];

const DB_TIME_SPENT_BUCKETS: &[f64; 8] = &[
    0.1, 0.2, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0
];

const SERIALIZATION_TIME_SPENT_BUCKETS: &[f64; 10] = &[
    0.01, 0.02, 0.03, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1.0
];

lazy_static! {
    pub static ref HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!("http_requests_total", "HTTP requests total"),
        &[]
    )
    .expect("Can't create a metric");
    pub static ref HTTP_REQUESTS_ERRORS: IntCounterVec = register_int_counter_vec!(
        opts!("http_requests_errors", "HTTP requests errors"),
        &[]
    )
    .expect("Can't create a metric");
    pub static ref HTTP_RESPONSE_TIME_SECONDS: HistogramVec = register_histogram_vec!(
        "http_response_time_seconds",
        "HTTP response times",
        &[],
        HTTP_RESPONSE_TIME_BUCKETS.to_vec()
    ).expect("Can't create a metric");
    pub static ref DB_TIME_SPENT_SECONDS: HistogramVec = register_histogram_vec!(
        "db_time_spent_seconds",
        "DB time spent seconds",
        &["table"],
        DB_TIME_SPENT_BUCKETS.to_vec()
    ).expect("Can't create a metric");
    pub static ref SERIALIZATION_TIME_SPENT_SECONDS: HistogramVec = register_histogram_vec!(
        "serialization_time_spent_seconds",
        "serialization time spent seconds",
        &[],
        SERIALIZATION_TIME_SPENT_BUCKETS.to_vec()
    ).expect("Can't create a metric");
}
