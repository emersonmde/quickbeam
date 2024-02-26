#![allow(dead_code)]
#![feature(test)]

use serde::Deserialize;
use serde::Serialize;
use std::sync::{Arc, RwLock};

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Row {
    id: i32,
    name: String,
}

type OptionalPageLock = Option<Arc<RwLock<Page>>>;
type PageVec = Vec<OptionalPageLock>;
type TablePagesLock = Arc<RwLock<PageVec>>;

#[derive(Debug, Clone)]
struct Pager {
    pages: TablePagesLock,
    num_rows: usize,
}

impl Pager {
    fn new() -> Self {
        let pages = Arc::new(RwLock::new(Vec::new()));
        Pager { pages, num_rows: 0 }
    }

    fn add_page(&self) {
        let mut pages = self.pages.write().unwrap();
        pages.push(Option::Some(Arc::new(RwLock::new(Page {
            count: 0,
            rows: Vec::new(),
        }))));
    }

    fn get_page(&self, page_num: usize) -> OptionalPageLock {
        let pages = self.pages.read().unwrap();
        pages.get(page_num).cloned()?
    }

    fn insert_row(&mut self, row: Row) -> Result<(), std::io::Error> {
        let page_num = self.num_rows / PAGE_SIZE;
        let row_num_in_page = self.num_rows % PAGE_SIZE;

        let pages_read_lock = self.pages.read().unwrap();
        let page_arc = if page_num < pages_read_lock.len() {
            // Page exists, clone Arc for later write access
            pages_read_lock[page_num].clone()
        } else {
            // Convert to write lock
            drop(pages_read_lock);
            let mut pages_write_lock = self.pages.write().unwrap();

            // Double check page wasn't already added
            if page_num >= pages_write_lock.len() {
                pages_write_lock.push(Some(Arc::new(RwLock::new(Page::new()))));
            }
            pages_write_lock[page_num].clone()
        };

        if let Some(page) = page_arc {
            let mut page_lock = page.write().unwrap();
            if row_num_in_page < PAGE_SIZE {
                page_lock.rows.push(row);
                self.num_rows += 1;
            } else {
                // TODO: Implement splitting pages
            }
        }

        Ok(())
    }

    fn get_row(&self, row_num: usize) -> Option<Row> {
        let page_num = row_num / PAGE_SIZE;
        let row_num = row_num % PAGE_SIZE;
        let page_lock = self.get_page(page_num)?;
        let page = page_lock.read().unwrap();
        page.get_row(row_num)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Page {
    count: usize,
    rows: Vec<Row>,
}

impl Page {
    fn new() -> Self {
        Page {
            count: 0,
            rows: Vec::new(),
        }
    }

    fn insert_row(&mut self, row: Row) {
        let rows = &mut self.rows;
        self.count += 1;
        rows.push(row);
    }

    fn get_row(&self, row_num: usize) -> Option<Row> {
        let rows = &self.rows;
        rows.get(row_num).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate test;

    #[test]
    fn test_table() {
        let table = Pager::new();
        assert_eq!(table.num_rows, 0);
    }

    #[test]
    fn test_insert_and_retrieve() {
        let mut table = Pager::new();
        let _ = table.insert_row(Row {
            id: 1,
            name: "test".to_string(),
        });
        let row = table.get_row(0).unwrap();
        assert_eq!(row.id, 1);
        assert_eq!(row.name, "test");
    }

    #[test]
    fn test_inserting_100000_rows() {
        let mut table = Pager::new();
        for i in 0..100_000 {
            table
                .insert_row(Row {
                    id: i,
                    name: format!("test{}", i),
                })
                .unwrap();
        }
        for i in 0..100_000 {
            let row = table.get_row(i).unwrap();
            assert_eq!(row.id, i.try_into().unwrap());
            assert_eq!(row.name, format!("test{}", i));
        }
        assert_eq!(table.num_rows, 100_000);
    }

    #[test]
    fn test_serializing_and_deserializing_pages() {
        let mut page = Page::new();
        page.insert_row(Row {
            id: 1,
            name: "test".to_string(),
        });
        page.insert_row(Row {
            id: 2,
            name: "test2".to_string(),
        });
        page.insert_row(Row {
            id: 3,
            name: "test3".to_string(),
        });
        let serialized = bincode::serialize(&page).unwrap();
        let deserialized: Page = bincode::deserialize(&serialized).unwrap();
        assert_eq!(page, deserialized);
    }

    #[bench]
    fn bench_inserting_1000_rows(b: &mut test::Bencher) {
        let mut table = Pager::new();
        b.iter(|| {
            for i in 0..1000 {
                table
                    .insert_row(Row {
                        id: i,
                        name: format!("test{}", i),
                    })
                    .unwrap();
            }
        });
    }

    #[bench]
    fn bench_retrieving_1000_rows(b: &mut test::Bencher) {
        let mut table = Pager::new();
        for i in 0..1000 {
            table
                .insert_row(Row {
                    id: i,
                    name: format!("test{}", i),
                })
                .unwrap();
        }
        b.iter(|| {
            for i in 0..1000 {
                let row = table.get_row(i).unwrap();
                assert_eq!(row.id, i.try_into().unwrap());
                assert_eq!(row.name, format!("test{}", i));
            }
        });
    }
}
