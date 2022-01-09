use crate::{Operations, Result};
use std::io::Write;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter},
    path::{Path, PathBuf},
};

const BLOCK_SIZE: usize = 256 * 1024;

pub struct LsmEngine {
    path: PathBuf,
    wal_writer: BufWriter<File>,
    memtable: Vec<String>,
}

impl LsmEngine {
    pub fn open(self, path: impl Into<PathBuf>) -> Result<LsmEngine> {
        let path = path.into();
        fs::create_dir_all(&*path)?;
        let wal_path = format!("{}/{}", path.to_string_lossy(), "wal.log");
        let wal_file = File::open(wal_path.clone())?;
        let memtable = self.restore_memtable(wal_file);
        let wal_file = File::open(wal_path)?;
        let wal_writer = BufWriter::new(wal_file);

        Ok(LsmEngine {
            path,
            wal_writer,
            memtable,
        })
    }

    fn restore_memtable(self, wal_file: File) -> Vec<String> {
        let wal_reader = BufReader::new(wal_file);
        wal_reader
            .lines()
            .map(|l| l.expect("Could not parse line"))
            .collect()
    }
}
impl Operations for LsmEngine {
    fn get(key: String) -> Result<Option<String>> {
        Ok(Some(String::from("Dummy")))
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        if self.memtable.len() > BLOCK_SIZE {
            // store memtable to disk
        }
        let entry = format!("{}{}", key, value);
        self.memtable.push(entry.clone());
        self.wal_writer.write_all(entry.as_bytes())?;
        Ok(())
    }
}
