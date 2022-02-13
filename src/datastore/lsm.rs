use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

use super::memtable::{MemTable, MemTableEntry, MemTableValue};
use crate::Result;
use std::io::Write;
use std::sync::Arc;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter},
    path::PathBuf,
};
use tokio::sync::Mutex;

const BLOCK_SIZE: usize = 1000;

#[derive(Clone)]
pub struct LsmEngine {
    path: Arc<PathBuf>,
    wal_writer: Arc<Mutex<BufWriter<File>>>,
    sst_counter: Arc<Mutex<usize>>,
    memtable: MemTable,
}

impl LsmEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<LsmEngine> {
        let path = path.into();
        fs::create_dir_all(&*path)?;
        let wal_path = format!("{}/{}", path.to_string_lossy(), "wal.log");
        let wal_file = File::open(wal_path.clone())?;
        let memtable = restore_memtable(wal_file)?;
        let wal_file = File::create(wal_path)?;
        let wal_writer = Arc::new(Mutex::new(BufWriter::new(wal_file)));

        Ok(LsmEngine {
            path: Arc::new(path),
            wal_writer,
            sst_counter: Arc::new(Mutex::new(0)),
            memtable,
        })
    }

    pub fn get(&self, key: String) -> Option<Entry<String, MemTableValue>> {
        self.memtable.get(key)
    }

    pub async fn set(&self, key: String, value: String, timestamp: u128) -> Result<()> {
        let entry = MemTableEntry {
            key,
            value,
            timestamp,
        };
        let serialized_entry = serde_json::to_string(&entry)?;
        let sst_counter_guard = self.sst_counter.clone();
        let path = self.path.clone();
        let wal_writer_guard = self.wal_writer.clone();
        let memtable = self.memtable.clone();
        if self.memtable.len() > BLOCK_SIZE {
            let mut sst_counter = sst_counter_guard.lock().await;

            let sst_path = format!("{}/sst/{}.log", path.to_string_lossy(), sst_counter);
            let sst_file = File::create(sst_path).unwrap();
            let mut sst_writer = BufWriter::new(sst_file);
            for map_entry in self.memtable.clone().entries().iter() {
                let key = map_entry.key().clone();
                let value = map_entry.value().clone().value;
                let timestamp = map_entry.value().timestamp;
                let entry = MemTableEntry {
                    key,
                    value,
                    timestamp,
                };
                let serialized_entry = serde_json::to_string(&entry).unwrap();
                sst_writer
                    .write_all(serialized_entry.as_bytes())
                    .expect("SST write failed.");
                sst_writer.write_all(b"\n").expect("SST write failed.");
            }
            sst_writer.flush().expect("SST flush failed");
            *sst_counter += 1;
            memtable.clear();
            memtable.set(entry);
            fs::remove_file("wal.log").unwrap();
            let wal_file = File::create("wal.log").unwrap();
            *wal_writer_guard.lock().await = BufWriter::new(wal_file);
            let mut wal_writer = wal_writer_guard.lock().await;
            wal_writer
                .write_all(serialized_entry.as_bytes())
                .expect("Error while writing to WAL");
            wal_writer
                .write_all(b"\n")
                .expect("Error while writing to WAL");
            wal_writer.flush().expect("Error while flushing");
        } else {
            self.memtable.set(entry);
            let mut wal_writer = wal_writer_guard.lock().await;
            wal_writer
                .write_all(serialized_entry.as_bytes())
                .expect("Error while writing to WAL");
            wal_writer
                .write_all(b"\n")
                .expect("Error while writing to WAL");
            wal_writer.flush().expect("Error while flushing");
        }
        Ok(())
    }
}

fn restore_memtable(wal_file: File) -> Result<MemTable> {
    let wal_reader = BufReader::new(wal_file);
    let entries: Arc<SkipMap<String, MemTableValue>> = Arc::new(SkipMap::new());
    let memtable = MemTable { entries };
    for line in wal_reader.lines() {
        let entry: MemTableEntry = serde_json::from_str(&line?)?;
        memtable.set(entry);
    }
    Ok(memtable)
}
