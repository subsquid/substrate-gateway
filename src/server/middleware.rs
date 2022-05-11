use std::future::{ready, Ready};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
use futures_util::future::LocalBoxFuture;
use tracing::info;

pub struct Logger;

impl<S, B> Transform<S, ServiceRequest> for Logger
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = LoggerMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(LoggerMiddleware { service }))
    }
}

pub struct LoggerMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for LoggerMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let fut = self.service.call(req);

        Box::pin(async move {
            let res = fut.await?;
            info!(
                client_ip_address = res.request().connection_info().peer_addr(),
                method = res.request().method().as_str(),
                path = res.request().path(),
                version = format!("{:?}", res.request().version()).as_str(),
                status = res.status().as_u16(),
            );
            Ok(res)
        })
    }
}
