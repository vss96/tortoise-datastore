use crate::Result;
use serde::{Deserialize, Serialize};

pub trait Operations {
    fn get(key: String) -> Result<Option<String>>;
    fn set(&mut self, key: String, value: String) -> Result<()>;
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: String,
    pub value: String,
    pub timestamp: u128,
}

mod index;
mod lsm;
mod memtable;

pub use self::index::Index;
pub use self::lsm::LsmEngine;
pub use self::memtable::MemTable;
