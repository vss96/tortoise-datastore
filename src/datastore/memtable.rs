use serde::{Deserialize, Serialize};
#[derive(Clone)]
pub struct MemTable {
    entries: Vec<MemTableEntry>,
    size: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MemTableEntry {
    pub key: String,
    pub value: String,
    pub timestamp: u128,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MemTableOperation {
    INSERTED,
    UPDATED,
    NONE,
}
impl MemTable {
    pub fn len(&self) -> usize {
        self.size
    }

    pub fn new(entries: Vec<MemTableEntry>, size: usize) -> Self {
        MemTable { entries, size }
    }

    pub fn set(&mut self, entry: MemTableEntry) -> MemTableOperation {
        return match self.get_index(entry.key.clone()) {
            Ok(idx) => {
                if self.entries[idx].timestamp > entry.timestamp {
                    return MemTableOperation::NONE;
                }

                if entry.value.len() < self.entries[idx].value.len() {
                    self.size -= self.entries[idx].value.len() - entry.value.len();
                } else {
                    self.size += entry.value.len() - self.entries[idx].value.len();
                }

                self.entries[idx] = entry;
                MemTableOperation::UPDATED
            }
            Err(idx) => {
                self.size += entry.key.len() + entry.value.len() + 16;
                self.entries.insert(idx, entry);
                MemTableOperation::INSERTED
            }
        };
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

    pub fn clear(&mut self) {
        self.entries.clear();
        self.size = 0;
    }

    pub fn entries(&self) -> Vec<MemTableEntry> {
        self.entries.clone()
    }
}

#[test]
fn memtable_test() {
    let mut memtable = MemTable::new(Vec::new(), 0);
    let entry1 = MemTableEntry {
        key: "123".to_string(),
        value: "456".to_string(),
        timestamp: 12345678,
    };
    assert_eq!(memtable.set(entry1.clone()), MemTableOperation::INSERTED);

    let entry2 = MemTableEntry {
        key: "12".to_string(),
        value: "789".to_string(),
        timestamp: 12345678,
    };
    assert_eq!(memtable.set(entry2.clone()), MemTableOperation::INSERTED);

    let entry3 = MemTableEntry {
        key: "12PE".to_string(),
        value: "7812A9".to_string(),
        timestamp: 12345678,
    };
    assert_eq!(memtable.set(entry3.clone()), MemTableOperation::INSERTED);

    let entry4 = MemTableEntry {
        key: "123".to_string(),
        value: "46".to_string(),
        timestamp: 12345678,
    };
    assert_eq!(memtable.set(entry4.clone()), MemTableOperation::UPDATED);

    let entry5 = MemTableEntry {
        key: "123".to_string(),
        value: "46".to_string(),
        timestamp: 123,
    };

    assert_eq!(memtable.set(entry5), MemTableOperation::NONE);

    assert_eq!(memtable.get("123".to_string()).unwrap(), entry4);
    assert_eq!(memtable.get("12".to_string()).unwrap(), entry2);

    assert_eq!(memtable.get_index("12PE".to_string()), Ok(2));

    assert_eq!(memtable.get_index("ABCD".to_string()), Err(3));
}
