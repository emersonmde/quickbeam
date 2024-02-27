#![allow(dead_code, unused_variables)]
#![feature(test)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};

const PAGE_SIZE: usize = 4096;
const ROWS_PER_PAGE: usize = PAGE_SIZE / std::mem::size_of::<Row>();

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Row {
    id: i32,
    name: String,
}

impl Default for Row {
    fn default() -> Self {
        Row {
            id: 0,
            name: String::new(),
        }
    }
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
        let rows: Vec<Row> = bincode::deserialize(bytes).unwrap();
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

            if file.metadata()?.len() < (offset + PAGE_SIZE as u64) {
                file.set_len(offset + PAGE_SIZE as u64)?;
            }

            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut buffer)?;
            drop(file);

            let page_lock = Arc::new(RwLock::new(Page::from_bytes(&buffer)));

            let mut pages = self.pages.write().unwrap();
            pages.insert(page_num, page_lock.clone());
            Ok(page_lock)
        }
    }

    fn flush_page(&self, page_num: usize) -> io::Result<()> {
        let pages = self.pages.read().unwrap();
        if let Some(page) = pages.get(&page_num) {
            let page = page.read().unwrap();
            let offset = (page_num * PAGE_SIZE) as u64;
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open("data.db")?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&bincode::serialize(&page.rows).unwrap())?;
            drop(file);
        }
        Ok(())
    }

    fn insert_row_at(&self, page_num: usize, row_num: usize, row: Row) -> io::Result<()> {
        let page = self.get_page(page_num)?;
        let mut page = page.write().unwrap();

        let index_within_page = row_num % ROWS_PER_PAGE;

        if index_within_page >= page.rows.len() {
            page.rows.resize_with(index_within_page + 1, Row::default);
        }

        page.rows[index_within_page] = row;
        eprintln!("Inserting row at page {}, row {}", page_num, row_num);
        Ok(())
    }

    fn get_row_at(&self, page_num: usize, row_num: usize) -> io::Result<Row> {
        let page = self.get_page(page_num)?;
        let page = page.read().unwrap();
        Ok(page.rows[row_num].clone())
    }

    // TODO: Replace with BTree search
    fn find(&self, key: i32) -> Option<Row> {
        let pages = self.pages.read().unwrap();
        for (page_num, page) in pages.iter() {
            let page = page.read().unwrap();
            for (row_num, row) in page.rows.iter().enumerate() {
                if row.id == key {
                    return Some(row.clone());
                }
            }
        }
        None
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

        self.pager
            .get_page(new_page_num)
            .expect("Failed to get page");

        if new_page_num != self.page_num {
            self.page_num = new_page_num;
        }
    }

    fn insert(&mut self, row: Row) {
        let page_num = self.row_num / ROWS_PER_PAGE;
        let _ = self.pager.insert_row_at(page_num, self.row_num, row);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_serialization_deserialization() {
        let row = Row {
            id: 1,
            name: "Test".to_string(),
        };
        let serialized = bincode::serialize(&row).unwrap();
        let deserialized: Row = bincode::deserialize(&serialized).unwrap();
        assert_eq!(row, deserialized);
    }

    #[test]
    fn test_page_initialization_and_byte_conversion() {
        let row = Row {
            id: 1,
            name: "Test".to_string(),
        };
        let mut page = Page::new();
        page.rows.push(row.clone());
        let page_bytes = bincode::serialize(&page.rows).unwrap();
        let reconstructed_page = Page::from_bytes(&page_bytes);
        assert_eq!(reconstructed_page.rows[0], row);
    }

    #[test]
    fn test_pager_get_and_flush_page() -> io::Result<()> {
        let pager = Pager::new()?;
        let page_lock = pager.get_page(0)?;
        {
            let mut page = page_lock.write().unwrap();
            page.rows.push(Row {
                id: 1,
                name: "Test".to_string(),
            });
        }
        pager.flush_page(0)?;
        Ok(())
    }

    #[test]
    fn test_pager_insert_row_and_get_row() -> io::Result<()> {
        let pager = Pager::new()?;
        let test_row = Row {
            id: 1,
            name: "Alice".to_string(),
        };
        pager.insert_row_at(0, 0, test_row.clone())?;
        let retrieved_row = pager.get_row_at(0, 0)?;
        assert_eq!(test_row, retrieved_row);
        Ok(())
    }

    #[test]
    fn test_pager_find() -> io::Result<()> {
        let pager = Pager::new()?;
        let test_row = Row {
            id: 1,
            name: "Alice".to_string(),
        };
        pager.insert_row_at(0, 0, test_row.clone())?;
        let found_row = pager.find(1);
        assert_eq!(Some(test_row), found_row);
        Ok(())
    }

    #[test]
    fn test_cursor_advance() -> io::Result<()> {
        let pager = Pager::new()?;
        let mut cursor = Cursor::new(Box::new(pager), 0)?;
        cursor.advance();
        assert_eq!(cursor.row_num, 1);
        Ok(())
    }

    #[test]
    fn test_cursor_insert() -> io::Result<()> {
        let pager = Pager::new()?;
        let mut cursor = Cursor::new(Box::new(pager), 0)?;
        let test_row = Row {
            id: 1,
            name: "Alice".to_string(),
        };
        cursor.insert(test_row.clone());
        let retrieved_row = cursor.pager.get_row_at(0, 0)?;
        assert_eq!(test_row, retrieved_row);
        Ok(())
    }

    #[test]
    fn test_find_after_cursor_insert_5_records() -> io::Result<()> {
        let pager = Pager::new()?;
        let mut cursor = Cursor::new(Box::new(pager), 0)?;
        let test_row = Row {
            id: 1,
            name: "test".to_string(),
        };
        for i in 0..50000 {
            cursor.insert(Row {
                id: i,
                name: "test".to_string(),
            });
            cursor.advance();
        }
        let found_row = cursor.pager.find(1);
        assert_eq!(Some(test_row), found_row);
        Ok(())
    }

    #[test]
    fn test_table_new() {
        let table = Table::new();
        assert!(table.pager.pages.read().unwrap().is_empty());
    }
}
