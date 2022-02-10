use tokio::io::AsyncWriteExt;

use super::memtable::{MemTable, MemTableEntry};
use crate::Result;
use std::io::Write;
use std::sync::Arc;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter},
    path::PathBuf,
};
use tokio::sync::Mutex;

const BLOCK_SIZE: usize = 256;

#[derive(Clone)]
pub struct LsmEngine {
    path: Arc<PathBuf>,
    wal_writer: Arc<Mutex<BufWriter<File>>>,
    sst_counter: Arc<Mutex<usize>>,
    memtable: Arc<Mutex<MemTable>>,
}

impl LsmEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<LsmEngine> {
        let path = path.into();
        fs::create_dir_all(&*path)?;
        let wal_path = format!("{}/{}", path.to_string_lossy(), "wal.log");
        let wal_file = File::open(wal_path.clone())?;
        let memtable = Arc::new(Mutex::new(restore_memtable(wal_file)?));
        let wal_file = File::create(wal_path)?;
        let wal_writer = Arc::new(Mutex::new(BufWriter::new(wal_file)));

        Ok(LsmEngine {
            path: Arc::new(path),
            wal_writer,
            sst_counter: Arc::new(Mutex::new(0)),
            memtable,
        })
    }

    pub async fn get(&self, key: String) -> Option<MemTableEntry> {
        self.memtable.lock().await.get(key)
    }

    pub async fn set(&self, key: String, value: String, timestamp: u128) -> Result<()> {
        let entry = MemTableEntry {
            key,
            value,
            timestamp,
        };
        let serialized_entry = serde_json::to_string(&entry)?;
        let memtable_guard = self.memtable.clone();

        let set_handle = tokio::spawn(async move {
            let mut memtable = memtable_guard.lock().await;
            memtable.set(entry).await;
        });
        let wal_writer_guard = self.wal_writer.clone();
        let sst_counter_guard = self.sst_counter.clone();
        let memtable_guard = self.memtable.clone();
        let path = self.path.clone();
        let disk_handle = tokio::spawn(async move {
            let mut memtable = memtable_guard.lock().await;
            if memtable.len() > BLOCK_SIZE {
                for entry in memtable.entries() {
                    let serialized_entry = serde_json::to_string(&entry).unwrap();
                    let mut sst_counter = sst_counter_guard.lock().await;
                    *sst_counter += 1;
                    let sst_path = format!("{}/sst/{}.log", path.to_string_lossy(), sst_counter);
                    let sst_file = File::create(sst_path).unwrap();
                    let mut sst_writer = BufWriter::new(sst_file);
                    sst_writer.write_all(serialized_entry.as_bytes()).unwrap();
                    sst_writer.write_all(b"\n").unwrap();
                    sst_writer.flush().unwrap();
                }
                memtable.clear();
                fs::remove_file("wal.log").unwrap();
                let wal_file = File::create("wal.log").unwrap();
                *wal_writer_guard.lock().await = BufWriter::new(wal_file);
            }
        });
        self.wal_writer
            .lock()
            .await
            .write_all(serialized_entry.as_bytes())?;
        self.wal_writer.lock().await.write_all(b"\n")?;
        self.wal_writer.lock().await.flush()?;
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
