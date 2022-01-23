use serde::{Deserialize, Serialize};
#[derive(Clone)]
pub struct MemTable {
    entries: Vec<MemTableEntry>,
    size: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemTableEntry {
    pub key: String,
    pub value: String,
    pub timestamp: u128,
}

impl MemTable {
    pub fn len(&self) -> usize {
        self.size
    }

    pub fn new(entries: Vec<MemTableEntry>, size: usize) -> Self {
        MemTable { entries, size }
    }

    pub fn set(&mut self, entry: MemTableEntry) {
        match self.get_index(entry.key.clone()) {
            Ok(idx) => {
                if self.entries[idx].timestamp > entry.timestamp {
                    return;
                }

                if entry.value.len() < self.entries[idx].value.len() {
                    self.size -= self.entries[idx].value.len() - entry.value.len();
                } else {
                    self.size += entry.value.len() - self.entries[idx].value.len();
                }

                self.entries[idx] = entry;
            }
            Err(idx) => {
                self.size += entry.key.len() + entry.value.len() + 16;
                self.entries.insert(idx, entry)
            }
        }
    }

    pub fn get_index(&self, key: String) -> Result<usize, usize> {
        self.entries.binary_search_by_key(&key, |e| e.key.clone())
    }

    pub fn get(&self, key: String) -> Option<MemTableEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(self.entries[idx].clone());
        }
        None
    }
}
