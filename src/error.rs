#[derive(Debug)]
pub enum Error {
    Internal(String),
}

impl std::convert::From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Error::Internal(err.to_string())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Internal(message) => write!(f, "{}", message),
        }
    }
}
