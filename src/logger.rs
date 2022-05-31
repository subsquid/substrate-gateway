use tracing_subscriber::EnvFilter;

fn is_tty() -> bool {
    unsafe { libc::isatty(libc::STDOUT_FILENO) != 0 }
}

pub fn init() {
    if is_tty() {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_target(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_target(false)
            .json()
            .flatten_event(true)
            .with_span_list(false)
            .with_current_span(false)
            .init();
    }
}
