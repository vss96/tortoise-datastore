use crate::Result;
pub trait Operations {
    fn get(key: String) -> Result<Option<String>>;
    fn set(&mut self, key: String, value: String) -> Result<()>;
}

mod lsm;
pub use self::lsm::LsmEngine;
