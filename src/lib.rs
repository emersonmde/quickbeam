#![allow(dead_code, unused_variables)]
#![feature(test)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
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

#[derive(Debug)]
struct Pager {
    pages: Arc<RwLock<HashMap<usize, PageLock>>>,
    file: File,
}

impl Pager {
    fn new() -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open("data.db")?;
        Ok(Pager {
            pages: Arc::new(RwLock::new(HashMap::new())),
            file,
        })
    }

    fn get_page(&mut self, page_num: usize) -> io::Result<PageLock> {
        // TODO: Stop pager from requiring write locks
        let mut pages = self.pages.write().unwrap();
        let page = pages
            .entry(page_num)
            .or_insert_with(|| {
                // Seek to the start of the page
                let offset = (page_num * PAGE_SIZE) as u64;
                self.file
                    .seek(SeekFrom::Start(offset))
                    .expect("Failed to seek in file");

                // Read the page data
                let mut buffer = vec![0; PAGE_SIZE];
                self.file
                    .read_exact(&mut buffer)
                    .expect("Failed to read page data");

                // Create a new Page from the buffer
                Arc::new(RwLock::new(Page::from_bytes(&buffer)))
            })
            .clone();
        Ok(page)
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
    pager: Arc<RwLock<Pager>>,
    current_page: Option<PageLock>,
    row_num: usize,
    end_of_table: bool,
}

impl Cursor {
    fn new(pager: Arc<RwLock<Pager>>, row_num: usize) -> Result<Self, io::Error> {
        let page_num = row_num / ROWS_PER_PAGE;
        // TODO: Stop pager from requiring write locks
        let mut pager_lock = pager.write().unwrap();
        let get_page = pager_lock.get_page(page_num)?;
        let current_page = Some(get_page);

        Ok(Cursor {
            pager: pager.clone(),
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
            // TODO: Stop pager from requiring write locks
            let mut pager_lock = self.pager.write().unwrap();
            self.current_page = Some(
                pager_lock
                    .get_page(new_page_num)
                    .expect("Failed to get page"),
            );
        }
    }
}
