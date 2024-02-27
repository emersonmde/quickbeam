#![allow(dead_code, unused_variables)]
#![feature(test)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, RwLock};

const PAGE_SIZE: usize = 4096;
const ROWS_PER_PAGE: usize = PAGE_SIZE / std::mem::size_of::<Row>();

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Row {
    id: i32,
    name: String,
}

#[derive(Debug, Clone)]
struct Page {
    rows: Vec<Row>,
}

impl Page {
    fn new() -> Self {
        Page { rows: Vec::new() }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut rows = Vec::new();
        for chunk in bytes.chunks_exact(std::mem::size_of::<Row>()) {
            let row: Row = bincode::deserialize(chunk).unwrap();
            rows.push(row);
        }
        Page { rows }
    }
}

type PageLock = Arc<RwLock<Page>>;

#[derive(Debug, Clone)]
struct Pager {
    pages: Arc<RwLock<HashMap<usize, PageLock>>>,
}

impl Pager {
    fn new() -> io::Result<Self> {
        Ok(Pager {
            pages: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn get_page(&self, page_num: usize) -> io::Result<PageLock> {
        // TODO: Stop pager from requiring write locks
        let pages = self.pages.read().unwrap();
        if let Some(page) = pages.get(&page_num) {
            Ok(page.clone())
        } else {
            drop(pages);

            let offset = (page_num * PAGE_SIZE) as u64;
            let mut buffer = vec![0; PAGE_SIZE];

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open("data.db")?;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut buffer)?;
            drop(file);

            let page_lock = Arc::new(RwLock::new(Page::from_bytes(&buffer)));

            let mut pages = self.pages.write().unwrap();
            pages.insert(page_num, page_lock.clone());
            Ok(page_lock)
        }
    }

    fn find(&self, key: i32) -> Option<usize> {
        todo!("Implement BTree Search");
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
    current_page: Option<PageLock>,
    row_num: usize,
    end_of_table: bool,
}

impl Cursor {
    fn new(pager: Box<Pager>, row_num: usize) -> Result<Self, io::Error> {
        let page_num = row_num / ROWS_PER_PAGE;
        let get_page = pager.get_page(page_num)?;
        let current_page = Some(get_page);

        Ok(Cursor {
            pager,
            current_page,
            row_num,
            end_of_table: false,
        })
    }

    fn advance(&mut self) {
        self.row_num += 1;
        let new_page_num = self.row_num / ROWS_PER_PAGE;

        if let Some(page_lock) = &self.current_page {
            let page = page_lock.read().unwrap();
            if self.row_num >= page.rows.len() * (new_page_num + 1) {
                self.end_of_table = true;
                return;
            }
        }

        if new_page_num != self.row_num / ROWS_PER_PAGE {
            self.current_page = Some(
                self.pager
                    .get_page(new_page_num)
                    .expect("Failed to get page"),
            );
        }
    }
}
