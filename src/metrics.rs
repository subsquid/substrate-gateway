use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use pin_project::pin_project;
use lazy_static::lazy_static;
use prometheus::{
    IntCounterVec, HistogramVec, HistogramTimer,
    register_int_counter_vec, register_histogram_vec, opts
};

const HTTP_RESPONSE_TIME_BUCKETS: &[f64; 8] = &[
    0.1, 0.2, 0.3, 0.5, 0.8, 1.0, 1.5, 2.0
];

const DB_TIME_SPENT_BUCKETS: &[f64; 8] = &[
    0.1, 0.5, 1.0, 3.0, 5.0, 10.0, 15.0, 30.0
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
        "HTTP response time",
        &[],
        HTTP_RESPONSE_TIME_BUCKETS.to_vec()
    ).expect("Can't create a metric");
    pub static ref DB_TIME_SPENT_SECONDS: HistogramVec = register_histogram_vec!(
        "db_time_spent_seconds",
        "db time spent",
        &["table"],
        DB_TIME_SPENT_BUCKETS.to_vec()
    ).expect("Can't create a metric");
}

#[pin_project]
pub struct Observer<Fut>
where
    Fut: Future,
{
    #[pin]
    inner: Fut,
    db_table: &'static str,
    timer: Option<HistogramTimer>,
}

impl<Fut> Future for Observer<Fut>
where
    Fut: Future,
{
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        this.timer.get_or_insert_with(|| {
            DB_TIME_SPENT_SECONDS.with_label_values(&[this.db_table])
                .start_timer()
        });
        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                this.timer.take()
                    .expect("timer is expected to be set at the polling start")
                    .observe_duration();
                Poll::Ready(v)
            }
        }
    }
}

pub trait ObserverExt: Sized + Future {
    fn observe_duration(self, db_table: &'static str) -> Observer<Self>
    {
        Observer {
            inner: self,
            db_table,
            timer: None,
        }
    }
}

impl<F: Future> ObserverExt for F {}
