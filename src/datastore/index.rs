use crate::datastore::Record;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

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
}
