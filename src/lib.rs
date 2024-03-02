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

type NodeId = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Leaf {
    parent_node: Option<NodeId>,
    size: usize,
    values: Vec<Row>,
    next_leaf: Option<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Internal {
    parent_node: Option<NodeId>,
    size: usize,
    children: Vec<NodeId>,
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
                // Uses binary search to find partition point
                leaf.values.get(leaf.values.partition_point(|v| v.id < key))
            },
            Node::Internal(_) => {
                panic!("Internal nodes should not contain rows")
            }
        }
    }

    fn insert_row(&mut self, key: i32, row: Row) {
        match &mut self.node {
            Node::Leaf(leaf) => {
                let idx = leaf.values.partition_point(|v| v.id < key);
                leaf.values.insert(idx, row);
            },
            Node::Internal(_) => {
                panic!("Internal nodes should not contain rows")
            }
        }
    }

    // Use self.binary_search to find and remove row
    fn remove_row(&mut self, _key: i32) {
        todo!("Implement remove_row")
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
    page_num: usize,
    row_num: usize,
    end_of_table: bool,
}

impl Cursor {
    fn new(pager: Box<Pager>, row_num: usize) -> Result<Self, io::Error> {
        let page_num = row_num / ROWS_PER_PAGE;

        Ok(Cursor {
            pager,
            page_num,
            row_num,
            end_of_table: false,
        })
    }

    fn advance(&mut self) {
        self.row_num += 1;
        let new_page_num = self.row_num / ROWS_PER_PAGE;

        if new_page_num != self.page_num {
            self.page_num = new_page_num;
        }
    }

    fn get_row(&mut self) -> Option<Row> {
        let page = self.pager.get_page(self.page_num).expect("Failed to get page");
        let row = page.get_row((self.row_num % ROWS_PER_PAGE).try_into().unwrap());
        row.cloned()
    }

    fn insert(&mut self, row: Row) {
        let page_num = self.row_num / ROWS_PER_PAGE;
        let page = self.pager.get_page(page_num).expect("Failed to get page");
        page.insert_row((self.row_num % ROWS_PER_PAGE).try_into().unwrap(), row);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate test;
    use std::fs;
    use test::Bencher;

    fn setup_test_db() -> io::Result<Pager> {
        let pager = Pager::new()?;
        std::fs::remove_file("data.db").ok();
        Ok(pager)
    }

    // Utility function to prepare a table with rows for reading benchmark
    fn prepare_table_with_rows(row_count: usize) -> Table {
        let table = Table::new();
        let mut cursor = Cursor::new(Box::new(table.pager.clone()), 0).unwrap();

        for i in 0..row_count {
            let row = Row {
                id: i as i32,
                name: format!("Test Name {}", i),
            };
            cursor.insert(row);
        }

        table
    }

    #[bench]
    fn bench_insert_rows(b: &mut Bencher) {
        // Clean up before running
        let _ = fs::remove_file("data.db");

        b.iter(|| {
            let table = Table::new();
            let mut cursor = Cursor::new(Box::new(table.pager.clone()), 0).unwrap();

            for i in 0..1000 {
                let row = Row {
                    id: i as i32,
                    name: format!("Test Name {}", i),
                };
                cursor.insert(row);
            }
        });

        // Cleanup after benchmark
        let _ = fs::remove_file("data.db");
    }
}
