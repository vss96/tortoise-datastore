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
    sst_counter: Arc<Mutex<u64>>,
    index: Arc<Index>,
}

impl LsmEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<LsmEngine> {
        let path = path.into();
        fs::create_dir_all(&*path)?;
        println!("Reached 1");
        let sst_count = get_sst_count(format!("{}/sst/", path.to_string_lossy()))?;
        println!("SST count : {}", sst_count);
        let index = restore_index( sst_count)?;

        Ok(LsmEngine {
            sst_counter: Arc::new(Mutex::new(sst_count + 1)),
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
        let sst_counter_guard = self.sst_counter.clone();
            tokio::spawn(async move {
                let mut sst_counter = sst_counter_guard.lock().await;

                let sst_path = format!("sst/{}.log",  sst_counter);
                *sst_counter += 1;
                drop(sst_counter);
                let sst_file = File::create(sst_path).unwrap();
                let mut sst_writer = BufWriter::new(sst_file);
                let serialized_entry = serde_json::to_string(&entry).unwrap();
                    sst_writer
                        .write_all(serialized_entry.as_bytes())
                        .expect("SST write failed.");
                    sst_writer.write_all(b"\n").expect("SST write failed.");

                sst_writer.flush().expect("SST flush failed");
               
            });
        Ok(())
    }
}

fn restore_index(sst_count: u64) -> Result<Index> {
    let index = Index::new(SkipMap::new());
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
