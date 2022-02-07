use super::memtable::{MemTable, MemTableEntry};
use crate::Result;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter},
    path::PathBuf,
};

const BLOCK_SIZE: usize = 1024 * 512;

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

    pub fn get(self, key: String) -> Option<MemTableEntry> {
        self.memtable.lock().unwrap().get(key)
    }

    pub fn set(&self, key: String, value: String, timestamp: u128) -> Result<()> {
        let entry = MemTableEntry {
            key,
            value,
            timestamp,
        };
        let mut memtable = self.memtable.lock().unwrap();
        if memtable.len() > BLOCK_SIZE {
            for entry in memtable.entries() {
                let serialized_entry = serde_json::to_string(&entry)?;
                let mut sst_counter = self.sst_counter.lock().unwrap();
                *sst_counter += 1;
                let sst_path = format!("{}/sst/{}.log", self.path.to_string_lossy(), sst_counter);
                let sst_file = File::create(sst_path)?;
                let mut writer = BufWriter::new(sst_file);
                writer.write_all(serialized_entry.as_bytes())?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }
            memtable.clear();
            fs::remove_file("wal.log")?;
            let wal_file = File::create("wal.log")?;
            *self.wal_writer.lock().unwrap() = BufWriter::new(wal_file);
            return Ok(());
        }
        let serialized_entry = serde_json::to_string(&entry)?;
        match memtable.set(entry) {
            INSERTED => {
                self.wal_writer
                    .lock()
                    .unwrap()
                    .write_all(serialized_entry.as_bytes())?;
                self.wal_writer.lock().unwrap().write_all(b"\n")?;
                self.wal_writer.lock().unwrap().flush()?;
            }
        }

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
