use std::sync::Arc;

use crossbeam_skiplist::{map::Entry, SkipMap};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct MemTable {
    pub entries: Arc<SkipMap<String, MemTableValue>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MemTableValue {
    pub value: String,
    pub timestamp: u128,
}

#[derive(Serialize, Deserialize, Debug)]
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
        self.entries.len()
    }

    pub fn set(&self, entry: MemTableEntry) -> MemTableOperation {
        let existing_value = self.get(entry.key.clone());
        return match existing_value {
            Some(existing_entry) => {
                if entry.timestamp < existing_entry.value().timestamp {
                    return MemTableOperation::NONE;
                }

                // if entry.value.len() < self.entries[idx].value.len() {
                //     self.size -= self.entries[idx].value.len() - entry.value.len();
                // } else {
                //     self.size += entry.value.len() - self.entries[idx].value.len();
                // }

                self.entries.insert(
                    entry.key,
                    MemTableValue {
                        value: entry.value,
                        timestamp: entry.timestamp,
                    },
                );
                MemTableOperation::UPDATED
            }
            None => {
                // self.size += entry.key.len() + entry.value.len() + 16;
                self.entries.insert(
                    entry.key,
                    MemTableValue {
                        value: entry.value,
                        timestamp: entry.timestamp,
                    },
                );
                MemTableOperation::INSERTED
            }
        };
    }

    pub fn get(&self, key: String) -> Option<Entry<String, MemTableValue>> {
        self.entries.get(&key)
    }

    pub fn clear(&self) {
        self.entries.clear();
    }

    pub fn entries(&self) -> Arc<SkipMap<String, MemTableValue>> {
        self.entries.clone()
    }
}

// #[test]
// fn memtable_test() {
//     let mut memtable = MemTable::new(Vec::new(), 0);
//     let entry1 = MemTableEntry {
//         key: "123".to_string(),
//         value: "456".to_string(),
//         timestamp: 12345678,
//     };
//     assert_eq!(memtable.set(entry1.clone()), MemTableOperation::INSERTED);

//     let entry2 = MemTableEntry {
//         key: "12".to_string(),
//         value: "789".to_string(),
//         timestamp: 12345678,
//     };
//     assert_eq!(memtable.set(entry2.clone()), MemTableOperation::INSERTED);

//     let entry3 = MemTableEntry {
//         key: "12PE".to_string(),
//         value: "7812A9".to_string(),
//         timestamp: 12345678,
//     };
//     assert_eq!(memtable.set(entry3.clone()), MemTableOperation::INSERTED);

//     let entry4 = MemTableEntry {
//         key: "123".to_string(),
//         value: "46".to_string(),
//         timestamp: 12345678,
//     };
//     assert_eq!(memtable.set(entry4.clone()), MemTableOperation::UPDATED);

//     let entry5 = MemTableEntry {
//         key: "123".to_string(),
//         value: "46".to_string(),
//         timestamp: 123,
//     };

//     assert_eq!(memtable.set(entry5), MemTableOperation::NONE);

//     assert_eq!(memtable.get("123".to_string()).unwrap(), entry4);
//     assert_eq!(memtable.get("12".to_string()).unwrap(), entry2);

//     assert_eq!(memtable.get_index("12PE".to_string()), Ok(2));

//     assert_eq!(memtable.get_index("ABCD".to_string()), Err(3));
// }
