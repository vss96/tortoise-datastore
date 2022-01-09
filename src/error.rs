use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatastoreError {
    #[error("Parsing error")]
    Serde(#[from] serde_json::Error),

    #[error("IO error")]
    IO(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, DatastoreError>;
