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
            file.write_all(&bincode::serialize(&page.rows).unwrap())?;
        }
        Ok(())
    }

    fn insert_row_at(&mut self, page_num: usize, row_num: usize, row: Row) -> io::Result<()> {
        let page = self.get_page(page_num)?;

        let index_within_page = row_num % ROWS_PER_PAGE;

        if index_within_page >= page.rows.len() {
            page.rows.resize_with(index_within_page + 1, Row::default);
        }

        page.rows[index_within_page] = row;
        eprintln!("Inserting row at page {}, row {}", page_num, row_num);
        Ok(())
    }

    fn get_row_at(&mut self, page_num: usize, row_num: usize) -> io::Result<Option<Row>> {
        match self.get_page(page_num) {
            Ok(page) => {
                let index_within_page = row_num % ROWS_PER_PAGE;
                if index_within_page < page.rows.len() {
                    Ok(Some(page.rows[index_within_page].clone()))
                } else {
                    Ok(None)
                }
            }
            Err(_) => Ok(None),
        }
    }

    // TODO: Replace with BTree search
    fn find(&self, key: i32) -> Option<Row> {
        for (_, page) in self.pages.iter() {
            for row in page.rows.iter() {
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
    extern crate test;
    use std::fs;
    use test::Bencher;

    fn setup_test_db() -> io::Result<Pager> {
        let pager = Pager::new()?;
        std::fs::remove_file("data.db").ok();
        Ok(pager)
    }

    #[test]
    fn test_insert_and_retrieve_single_row() -> io::Result<()> {
        let mut pager = setup_test_db()?;
        let row = Row {
            id: 1,
            name: "Test User".to_string(),
        };
        pager.insert_row_at(0, 0, row.clone())?;

        let retrieved_row = pager.get_row_at(0, 0)?;
        assert_eq!(retrieved_row, Some(row));

        Ok(())
    }

    #[test]
    fn test_insert_and_retrieve_multiple_rows_across_pages() -> io::Result<()> {
        let mut pager = setup_test_db()?;
        let row1 = Row {
            id: 1,
            name: "Test User 1".to_string(),
        };
        let row2 = Row {
            id: 2,
            name: "Test User 2".to_string(),
        };

        let second_page_row_num = ROWS_PER_PAGE;

        pager.insert_row_at(0, 0, row1.clone())?;
        pager.insert_row_at(1, second_page_row_num, row2.clone())?;

        let retrieved_row1 = pager.get_row_at(0, 0)?;
        let retrieved_row2 = pager.get_row_at(1, second_page_row_num)?;

        assert_eq!(retrieved_row1, Some(row1));
        assert_eq!(retrieved_row2, Some(row2));

        Ok(())
    }

    #[test]
    fn test_persistence() -> io::Result<()> {
        {
            let mut pager = setup_test_db()?;
            let row = Row {
                id: 1,
                name: "Persisted User".to_string(),
            };
            pager.insert_row_at(0, 0, row)?;
            pager.flush_page(0)?;
            // Ensure pager is dropped and data is flushed to disk
        }

        // Reload and check
        {
            let mut pager = Pager::new()?;
            let retrieved_row = pager.get_row_at(0, 0)?;
            assert_eq!(
                retrieved_row,
                Some(Row {
                    id: 1,
                    name: "Persisted User".to_string()
                })
            );
        }

        Ok(())
    }

    #[test]
    fn test_insert_beyond_current_size() -> io::Result<()> {
        let mut pager = setup_test_db()?;
        let row = Row {
            id: 1,
            name: "Far User".to_string(),
        };

        // Insert a row well beyond the current size to test auto-extension
        let far_row_num = ROWS_PER_PAGE * 10;
        pager.insert_row_at(10, far_row_num, row.clone())?;

        let retrieved_row = pager.get_row_at(10, far_row_num)?;
        assert_eq!(retrieved_row, Some(row));

        Ok(())
    }

    #[bench]
    fn bench_insert_row(b: &mut Bencher) {
        let mut pager = Pager::new().unwrap();
        let row = Row {
            id: 1,
            name: "Test Name".to_string(),
        };

        // Clean up before running
        let _ = fs::remove_file("data.db");

        b.iter(|| {
            // Note: This simplistic approach does not manage page or row numbers
            // realistically. Adjust according to your implementation needs.
            let _ = pager.insert_row_at(0, 0, row.clone());
        });

        // Cleanup after benchmark
        let _ = fs::remove_file("data.db");
    }

    #[bench]
    fn bench_get_row(b: &mut Bencher) {
        let mut pager = Pager::new().unwrap();
        let row = Row {
            id: 1,
            name: "Test Name".to_string(),
        };
        let _ = pager.insert_row_at(0, 0, row.clone());

        b.iter(|| {
            let _ = pager.get_row_at(0, 0);
        });

        // Cleanup after benchmark
        let _ = fs::remove_file("data.db");
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

    #[bench]
    fn bench_read_rows(b: &mut Bencher) {
        // Prepare table with rows for reading
        let table = prepare_table_with_rows(1000);

        b.iter(|| {
            let mut read_cursor = Cursor::new(Box::new(table.pager.clone()), 0).unwrap();

            for _ in 0..1000 {
                if let Some(row) = read_cursor
                    .pager
                    .get_row_at(read_cursor.page_num, read_cursor.row_num)
                    .unwrap()
                {
                    test::black_box(row);
                }
                read_cursor.advance();
            }
        });
    }
}
