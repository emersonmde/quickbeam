#![feature(test)]
#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};

const PAGE_SIZE: usize = 4096;
const ROWS_PER_PAGE: usize = PAGE_SIZE / std::mem::size_of::<Row>();

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
struct Row {
    id: i32,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Node {
    Leaf(Leaf),
    Internal(Internal),
}

impl Node {
    fn get_row(&self, key: i32) -> Option<&Row> {
        match self {
            Node::Leaf(leaf) => leaf.get_row(key),
            Node::Internal(_) => panic!("Internal nodes should not contain rows"),
        }
    }
}

type NodeId = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Leaf {
    parent_node: Option<NodeId>,
    size: usize,
    values: Vec<Row>,
    next_leaf: Option<NodeId>,
}

impl Leaf {
    fn get_row(&self, key: i32) -> Option<&Row> {
        // Uses binary search to find partition point
        self.values.get(self.values.partition_point(|v| v.id < key))
    }

    fn insert_row(&mut self, key: i32, row: Row) {
        let idx = self.values.partition_point(|v| v.id < key);
        self.values.insert(idx, row);
    }

    fn remove_row(&mut self, _key: i32) {
        todo!("Implement remove_row")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Internal {
    parent_node: Option<NodeId>,
    size: usize,
    // (child_node_id, max_key)
    children: Vec<(NodeId, i32)>,
}

impl Internal {
    fn get_child_num(&self, key: i32) -> usize {
        self.children.partition_point(|v| v.1 < key)
    }
}

#[derive(Debug, Clone)]
struct Page {
    node: Node,
}

impl Page {
    fn new() -> Self {
        Page {
            node: Node::Leaf(Leaf {
                parent_node: None,
                size: 0,
                values: Vec::new(),
                next_leaf: None,
            }),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let node: Node = bincode::deserialize(bytes).unwrap();
        Page { node }
    }

    fn get_row(&self, key: i32) -> Option<&Row> {
        match &self.node {
            Node::Leaf(leaf) => {
                leaf.get_row(key)
            }
            Node::Internal(_) => {
                panic!("Internal nodes should not contain rows")
            }
        }
    }

    fn insert_row(&mut self, key: i32, row: Row) {
        match &mut self.node {
            Node::Leaf(leaf) => {
                leaf.insert_row(key, row)
            }
            Node::Internal(_) => {
                panic!("Internal nodes should not contain rows")
            }
        }
    }

    // Use self.binary_search to find and remove row
    fn remove_row(&mut self, key: i32) {
        match &mut self.node {
            Node::Leaf(leaf) => {
                leaf.remove_row(key)
            }
            Node::Internal(_) => {
                panic!("Internal nodes should not contain rows")
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Pager {
    pages: HashMap<usize, Page>,
}

impl Pager {
    fn new() -> io::Result<Self> {
        Ok(Pager {
            pages: HashMap::new(),
        })
    }

    fn get_page(&mut self, page_num: usize) -> io::Result<&mut Page> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.pages.entry(page_num) {
            let offset = (page_num * PAGE_SIZE) as u64;
            let mut buffer = vec![0; PAGE_SIZE];
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open("data.db")?;

            if file.metadata()?.len() < (offset + PAGE_SIZE as u64) {
                file.set_len(offset + PAGE_SIZE as u64)?;
            }

            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut buffer)?;
            let page = Page::from_bytes(&buffer);
            e.insert(page);
        }
        Ok(self.pages.get_mut(&page_num).unwrap())
    }

    fn flush_page(&self, page_num: usize) -> io::Result<()> {
        if let Some(page) = self.pages.get(&page_num) {
            let offset = (page_num * PAGE_SIZE) as u64;
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open("data.db")?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&bincode::serialize(&page.node).unwrap())?;
        }
        Ok(())
    }

    fn find_page_by_key(&mut self, key: i32) -> Option<Page> {
        let mut page_num = 0;
        let mut page = self.get_page(page_num).unwrap();
        let mut node = match &page.node {
            Node::Leaf(_) => panic!("Root should be an internal node"),
            Node::Internal(internal) => internal,
        };
    
        loop {
            let child_num = node.get_child_num(key);
            page_num = node.children[child_num].0;
            page = self.get_page(page_num).unwrap();
            node = match &page.node {
                Node::Leaf(_) => return Some(page.clone()),
                Node::Internal(internal) => internal,
            };
        }
    }

    fn find_row_by_key(&mut self, key: i32) -> Option<Row> {
        let page = self.find_page_by_key(key)?;
        page.get_row(key).cloned()
    }

    fn insert_row(&mut self, key: i32, row: Row) {
        let mut page = self.find_page_by_key(key).unwrap();
        page.insert_row(key, row);
        // TODO - Split page if necessary
    }
}

#[derive(Debug)]
struct Table {
    pager: Pager,
}

impl Table {
    fn new() -> Self {
        Table {
            pager: Pager::new().expect("Error opening database file"),
        }
    }
}

struct Cursor {
    pager: Box<Pager>,
    keys: Vec<i32>,
    current_idx: usize,
}

impl Cursor {
    fn new(mut pager: Box<Pager>, keys: Vec<i32>) -> Result<Self, io::Error> {
        let current_idx = pager.get_page(0)?.node.get_row(keys[0]).is_some() as usize;
        Ok(Cursor {
            pager,
            keys,
            current_idx
        })
    }

    fn advance(&mut self) {
        self.current_idx += 1;
    }

    fn get_row(&mut self) -> Option<Row> {
        self.pager.find_row_by_key(self.keys[self.current_idx])
    }

    fn insert(&mut self, row: Row) {
        let mut page = self.pager.find_page_by_key(self.keys[self.current_idx])
            .expect("Unable to get page");
        page.insert_row(self.keys[self.current_idx], row);
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    extern crate test;
}
