use super::memtable::{MemTable, MemTableEntry};
use crate::{Operations, Result};
use crossbeam_skiplist::SkipMap;
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
    memtable: MemTable,
}

impl LsmEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<LsmEngine> {
        let path = path.into();
        fs::create_dir_all(&*path)?;
        let wal_path = format!("{}/{}", path.to_string_lossy(), "wal.log");
        let wal_file = File::open(wal_path.clone())?;
        let memtable = restore_memtable(wal_file)?;
        let wal_file = File::open(wal_path)?;
        let wal_writer = BufWriter::new(wal_file);

        Ok(LsmEngine {
            path,
            wal_writer,
            memtable,
        })
    }

    pub fn get(self, key: String) -> Option<MemTableEntry> {
        self.memtable.get(key)
    }

    pub fn set(&mut self, key: String, value: String, timestamp: u128) -> Result<()> {
        let entry = MemTableEntry {
            key,
            value,
            timestamp,
        };
        if self.memtable.len() > BLOCK_SIZE {
            // store memtable to disk
        }
        let serialized_entry = serde_json::to_string(&entry)?;
        self.memtable.set(entry);
        self.wal_writer.write_all(serialized_entry.as_bytes())?;
        Ok(())
    }
}

fn restore_memtable(wal_file: File) -> Result<MemTable> {
    let wal_reader = BufReader::new(wal_file);
    let entries: Vec<MemTableEntry> = wal_reader
        .lines()
        .map(|l| {
            serde_json::from_str(&l.expect("error mapping line"))
                .expect("Couldn't deserialize string")
        })
        .collect();
    let size: usize = entries
        .iter()
        .map(|x| x.value.len() + x.key.len() + 16)
        .sum();
    Ok(MemTable::new(entries, size))
}
