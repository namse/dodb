use std::collections::BTreeMap;

use crate::{DocumentKey, Revision};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteIntent {
    Put(Vec<u8>),
    Delete,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReadSet(BTreeMap<DocumentKey, Revision>);

impl ReadSet {
    pub fn record(&mut self, key: DocumentKey, revision: Revision) {
        self.0.entry(key).or_insert(revision);
    }

    pub fn get(&self, key: &DocumentKey) -> Option<Revision> {
        self.0.get(key).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&DocumentKey, &Revision)> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteSet(BTreeMap<DocumentKey, WriteIntent>);

impl WriteSet {
    pub fn put(&mut self, key: DocumentKey, value: impl Into<Vec<u8>>) {
        self.0.insert(key, WriteIntent::Put(value.into()));
    }

    pub fn delete(&mut self, key: DocumentKey) {
        self.0.insert(key, WriteIntent::Delete);
    }

    pub fn get(&self, key: &DocumentKey) -> Option<&WriteIntent> {
        self.0.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&DocumentKey, &WriteIntent)> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
