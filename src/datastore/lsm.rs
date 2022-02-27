use crossbeam_skiplist::SkipMap;
use tokio::io::AsyncWriteExt;

use super::memtable::MemTable;
use crate::datastore::{Index, Record};
use crate::Result;
use crossbeam_skiplist::map::Entry;
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
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
    sst_counter: Arc<Mutex<u64>>,
    memtable: Arc<Mutex<MemTable>>,
    index: Arc<Index>,
}

impl LsmEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<LsmEngine> {
        let path = path.into();
        fs::create_dir_all(&*path)?;
        let wal_path = format!("{}/{}", path.to_string_lossy(), "wal.log");
        println!("Reached 1");
        let wal_file = File::open(wal_path.clone())?;
        let memtable = restore_memtable(wal_file)?;
        let wal_file = OpenOptions::new().append(true).open(wal_path.clone())?;

        let wal_writer = Arc::new(Mutex::new(BufWriter::new(wal_file)));
        println!("Reached 2");
        let sst_count = get_sst_count(format!("{}/sst/", path.to_string_lossy()))?;
        println!("Reached 3 : {}", sst_count);
        let index = restore_index(wal_path, sst_count, memtable.clone())?;
        // println!("{:?}", index.get("1206".to_string()).unwrap());

        Ok(LsmEngine {
            path: Arc::new(path),
            wal_writer,
            sst_counter: Arc::new(Mutex::new(sst_count + 1)),
            memtable: Arc::new(Mutex::new(memtable)),
            index: Arc::new(index),
        })
    }

    pub fn get(&self, key: String) -> Option<Entry<'_, String, Record>> {
        self.index.get(key)
    }

    pub async fn set(&self, key: String, value: String, timestamp: u128) -> Result<()> {
        let entry = Record {
            key,
            value,
            timestamp,
        };
        let serialized_entry = serde_json::to_string(&entry)?;
        let sst_counter_guard = self.sst_counter.clone();
        let path = self.path.clone();
        let memtable_guard = self.memtable.clone();
        let wal_writer_guard = self.wal_writer.clone();
        if memtable_guard.lock().await.len() > BLOCK_SIZE {
            tokio::spawn(async move {
                let mut memtable = memtable_guard.lock().await;

                let mut sst_counter = sst_counter_guard.lock().await;

                let sst_path = format!("{}/sst/{}.log", path.to_string_lossy(), sst_counter);
                let sst_file = File::create(sst_path).unwrap();
                let mut sst_writer = BufWriter::new(sst_file);
                for entry in memtable.entries() {
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
            });
        } else {
            let mut memtable = self.memtable.lock().await;
            memtable.set(entry);
            drop(memtable);
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
    let entries: Vec<Record> = wal_reader
        .lines()
        .map(|l| {
            serde_json::from_str(&l.expect("error mapping line"))
                .expect("Couldn't deserialize string")
        })
        .collect();
    let size: usize = entries.len();
    println!("Wal Size: {}", size);
    Ok(MemTable::new(entries, size))
}

fn restore_index(wal_path: String, sst_count: u64, memtable: MemTable) -> Result<Index> {
    let index = Index::new(SkipMap::new());
    let wal_reader = BufReader::new(File::open(wal_path)?);
    index.read_memtable(memtable);
    println!("Index size: {}", index.len());
    for count in 1..sst_count + 1 {
        let sst_path = format!("sst/{}.log", count);
        println!("File count: {}", count);
        match File::open(sst_path) {
            Ok(file) => {
                read_file(&index, BufReader::new(file));
                println!("Index size: {}", index.len());
            }
            Err(_e) => continue,
        };
    }
    Ok(index)
}

fn read_file(index: &Index, reader: BufReader<File>) {
    for record in reader.lines().into_iter().map(|l| {
        serde_json::from_str(&l.expect("error mapping line")).expect("Couldn't deserialize string")
    }) {
        index.set(record);
    }
}

fn get_sst_count(sst_path: String) -> Result<u64> {
    let sst_count = fs::read_dir(&sst_path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .max_by_key(|x| x.clone())
        .map_or_else(|| 0, |v| v);
    Ok(sst_count)
}
