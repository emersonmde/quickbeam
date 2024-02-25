#![allow(dead_code)]

use std::{cell::RefCell, fmt, sync::{Arc, RwLock}};

use serde::{de::{SeqAccess, Visitor}, Deserialize, Deserializer, Serialize, Serializer};
use serde::ser::SerializeStruct;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;


#[derive(Serialize, Deserialize, Debug, Clone)]
struct Row {
    id: i32,
    name: String,
}


#[derive(Debug, Clone)]
struct Table {
    pages: Arc<RwLock<Vec<Page>>>,
    num_rows: usize,
}

impl Table {
    fn new() -> Self {
        let pages = Arc::new(RwLock::new(Vec::new()));
        Table {
            pages,
            num_rows: 0,
        }
    }

    fn add_page(&self) {
        let mut pages = self.pages.write().unwrap();
        pages.push(Page {
            count: 0,
            rows: RefCell::new(Vec::new()),
        });
    }

    fn get_page(&self, page_num: usize) -> Option<Page> {
        let pages = self.pages.read().unwrap();
        pages.get(page_num).cloned()
    }

    fn insert_row(&self, row: Row) {
        let pages = self.pages.write().unwrap();
        let last_page = pages.last().unwrap();
        last_page.insert_row(row);
    }

    fn get_row(&self, row_num: usize) -> Option<Row> {
        let page_num = row_num / PAGE_SIZE;
        let row_num = row_num % PAGE_SIZE;
        let page = self.get_page(page_num)?;
        page.get_row(row_num)
    }
}

impl Serialize for Table {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let pages = self.pages.read().unwrap();
        let mut ser = serializer.serialize_struct("Table", 2)?;
        ser.serialize_field("pages", &*pages)?;
        ser.serialize_field("num_rows", &self.num_rows)?;
        ser.end()
    }
}

impl<'de> Deserialize<'de> for Table {
    fn deserialize<D>(deserializer: D) -> Result<Table, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TableVisitor;

        impl<'de> Visitor<'de> for TableVisitor {
            type Value = Table;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Table")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Table, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut pages: Vec<Page> = Vec::new();
                let mut num_rows: usize = 0;
                while let Some(key) = map.next_key()? {
                    match key {
                        "pages" => {
                            pages = map.next_value()?;
                        }
                        "num_rows" => {
                            num_rows = map.next_value()?;
                        }
                        _ => {}
                    }
                }
                Ok(Table {
                    pages: Arc::new(RwLock::new(pages)),
                    num_rows,
                })
            }
        }

        deserializer.deserialize_struct("Table", &["pages", "num_rows"], TableVisitor)
    }
}

#[derive(Debug, Clone)]
struct Page {
    count: usize,
    rows: RefCell<Vec<Row>>,
}

impl Page {
    fn new() -> Self {
        Page {
            count: 0,
            rows: RefCell::new(Vec::new()),
        }
    }

    fn insert_row(&self, row: Row) {
        let mut rows = self.rows.borrow_mut();
        rows.push(row);
    }

    fn get_row(&self, row_num: usize) -> Option<Row> {
        let rows = self.rows.borrow();
        rows.get(row_num).cloned()
    }
}


impl Serialize for Page {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let rows = self.rows.borrow();
        let mut state = serializer.serialize_struct("Page", 1)?;
        state.serialize_field("rows", &*rows)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Page {
    fn deserialize<D>(deserializer: D) -> Result<Page, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PageVisitor;

        impl<'de> Visitor<'de> for PageVisitor {
            type Value = Page;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Page")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Page, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let rows: Vec<Row> = seq.next_element()?.ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                Ok(Page {
                    count: rows.len(),
                    rows: RefCell::new(rows),
                })
            }
        }
        
        deserializer.deserialize_seq(PageVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(true, true);
    }

    #[test]
    fn test_table() {
        let table = Table::new();
        table.add_page();
        let page = table.get_page(0).unwrap();
        page.insert_row(Row {
            id: 1,
            name: "test".to_string(),
        });
        let row = page.get_row(0).unwrap();
        assert_eq!(row.id, 1);
        assert_eq!(row.name, "test");
    }

    #[test]
    fn test_insert_and_retrieve() {
        let table = Table::new();
        table.add_page();
        let page = table.get_page(0).unwrap();
        page.insert_row(Row {
            id: 1,
            name: "test1".to_string(),
        });
        page.insert_row(Row {
            id: 2,
            name: "test2".to_string(),
        });
        let row = page.get_row(1).unwrap();
        assert_eq!(row.id, 2);
        assert_eq!(row.name, "test2");
    }

    #[test]
    fn test_inserting_2000_rows() {
        let table = Table::new();
        table.add_page();
        for i in 0..2000 {
            table.insert_row(Row {
                id: i as i32,
                name: format!("test{}", i),
            });
        }
        for i in 0..2000 {
            let row = table.get_row(i);
            if let Some(row) = row {
                assert_eq!(row.id, i as i32);
                assert_eq!(row.name, format!("test{}", i));
            } else {
                eprintln!("Row id {} not found", i);
                assert!(false);
            }
        }
    }










}
