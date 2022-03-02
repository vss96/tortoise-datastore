use crate::datastore::Record;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

use super::MemTable;

pub struct Index {
    lookup: SkipMap<String, Record>,
}

impl Index {
    pub fn new(lookup: SkipMap<String, Record>) -> Self {
        Self { lookup }
    }
    pub fn get(&self, key: String) -> Option<Entry<'_, String, Record>> {
        self.lookup.get(&key)
    }

    pub fn set(&self, entry: Record) {
        let existing_record = self.get(entry.key.clone());
        if let Some(record) = existing_record {
            if entry.timestamp < record.value().timestamp {
                return;
            }
        }
        self.lookup.insert(entry.key.clone(), entry);
    }

    pub fn len(&self) -> usize {
        self.lookup.len()
    }

    pub fn read_memtable(&self, memtable: MemTable) {
        for record in memtable.entries() {
            self.set(record);
        }
    }
}
