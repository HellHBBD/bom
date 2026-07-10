use std::collections::HashSet;

use rust_decimal::Decimal;

#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{params, Connection, OptionalExtension, Transaction};

#[cfg(not(target_arch = "wasm32"))]
use crate::db::open_manual_write_database;
use crate::decimal::{normalize_decimal_text, parse_decimal_field};
use crate::error::{AppError, AppResult};

#[derive(Clone, Debug, PartialEq)]
pub struct BatchPriceInput {
    pub price_date: String,
    pub rows: Vec<BatchPriceRowInput>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchPriceRowInput {
    pub instrument_id: i64,
    pub symbol: String,
    pub instrument_name: String,
    pub currency_code: String,
    pub price: String,
}

#[derive(Clone, Debug, PartialEq)]
struct ValidatedBatchPriceInput {
    price_date: String,
    rows: Vec<ValidatedBatchPriceRow>,
}

#[derive(Clone, Debug, PartialEq)]
struct ValidatedBatchPriceRow {
    instrument_id: i64,
    symbol: String,
    instrument_name: String,
    currency_code: String,
    price_text: String,
}

#[allow(dead_code)]
pub fn validate_batch_prices(input: &BatchPriceInput) -> AppResult<BatchPriceInput> {
    let validated = validate_batch_prices_inner(input)?;

    Ok(BatchPriceInput {
        price_date: validated.price_date,
        rows: validated
            .rows
            .into_iter()
            .map(|row| BatchPriceRowInput {
                instrument_id: row.instrument_id,
                symbol: row.symbol,
                instrument_name: row.instrument_name,
                currency_code: row.currency_code,
                price: row.price_text,
            })
            .collect(),
    })
}

#[allow(dead_code)]
#[cfg(not(target_arch = "wasm32"))]
pub fn upsert_manual_prices_batch(input: BatchPriceInput) -> AppResult<usize> {
    let mut connection = open_manual_write_database()?;
    upsert_manual_prices_batch_with_connection(&mut connection, input)
}

#[cfg(target_arch = "wasm32")]
pub fn upsert_manual_prices_batch(_input: BatchPriceInput) -> AppResult<usize> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 市價更新".to_string(),
    ))
}

fn validate_batch_prices_inner(input: &BatchPriceInput) -> AppResult<ValidatedBatchPriceInput> {
    let price_date = input.price_date.trim().to_string();
    if price_date.is_empty() {
        return Err(AppError::Validation("請輸入價格日期".to_string()));
    }
    if !is_iso_date(&price_date) {
        return Err(AppError::Validation(
            "價格日期格式必須為 YYYY-MM-DD".to_string(),
        ));
    }

    let mut seen_instruments = HashSet::new();
    let mut validated_rows = Vec::new();

    for row in &input.rows {
        let trimmed_price = row.price.trim();
        if trimmed_price.is_empty() {
            continue;
        }

        if row.instrument_id <= 0 {
            return Err(AppError::Validation(format!(
                "{} 缺少有效的商品 ID",
                display_row_label(row)
            )));
        }

        let currency_code = row.currency_code.trim().to_string();
        if currency_code.is_empty() {
            return Err(AppError::Validation(format!(
                "{} 缺少幣別",
                display_row_label(row)
            )));
        }

        let price = parse_decimal_field("price", trimmed_price).map_err(|error| match error {
            AppError::InvalidDecimal { .. } => {
                AppError::Validation(format!("{} 的新市價格式錯誤", display_row_label(row)))
            }
            other => other,
        })?;
        if price <= Decimal::ZERO {
            return Err(AppError::Validation(format!(
                "{} 的新市價必須大於 0",
                display_row_label(row)
            )));
        }

        if !seen_instruments.insert(row.instrument_id) {
            return Err(AppError::Validation(format!(
                "同一批次不可重複輸入 {} 的價格",
                display_row_label(row)
            )));
        }

        validated_rows.push(ValidatedBatchPriceRow {
            instrument_id: row.instrument_id,
            symbol: row.symbol.trim().to_string(),
            instrument_name: row.instrument_name.trim().to_string(),
            currency_code,
            price_text: normalize_decimal_text(price),
        });
    }

    if validated_rows.is_empty() {
        return Err(AppError::Validation("沒有要儲存的價格".to_string()));
    }

    Ok(ValidatedBatchPriceInput {
        price_date,
        rows: validated_rows,
    })
}

fn display_row_label(row: &BatchPriceRowInput) -> String {
    let symbol = row.symbol.trim();
    let instrument_name = row.instrument_name.trim();

    match (symbol.is_empty(), instrument_name.is_empty()) {
        (false, false) => format!("{} {}", symbol, instrument_name),
        (false, true) => symbol.to_string(),
        (true, false) => instrument_name.to_string(),
        (true, true) => format!("商品 #{}", row.instrument_id),
    }
}

fn is_iso_date(value: &str) -> bool {
    let mut segments = value.split('-');
    let (Some(year), Some(month), Some(day), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return false;
    };

    if year.len() != 4 || month.len() != 2 || day.len() != 2 {
        return false;
    }

    let Ok(year) = year.parse::<i32>() else {
        return false;
    };
    let Ok(month) = month.parse::<u32>() else {
        return false;
    };
    let Ok(day) = day.parse::<u32>() else {
        return false;
    };

    if !(1..=12).contains(&month) {
        return false;
    }

    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return false,
    };

    (1..=max_day).contains(&day)
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[allow(dead_code)]
#[cfg(not(target_arch = "wasm32"))]
fn upsert_manual_prices_batch_with_connection(
    connection: &mut Connection,
    input: BatchPriceInput,
) -> AppResult<usize> {
    let validated = validate_batch_prices_inner(&input)?;
    let transaction = connection.transaction()?;

    for row in &validated.rows {
        upsert_manual_price(&transaction, &validated.price_date, row)?;
    }

    transaction.commit()?;
    Ok(validated.rows.len())
}

#[allow(dead_code)]
#[cfg(not(target_arch = "wasm32"))]
fn upsert_manual_price(
    transaction: &Transaction<'_>,
    price_date: &str,
    row: &ValidatedBatchPriceRow,
) -> AppResult<()> {
    let existing_id: Option<i64> = transaction
        .query_row(
            r#"
            SELECT price_id
            FROM instrument_price
            WHERE instrument_id = ?1
              AND price_date = ?2
              AND origin = 'MANUAL'
            ORDER BY price_id DESC
            LIMIT 1
            "#,
            params![row.instrument_id, price_date],
            |result_row| result_row.get(0),
        )
        .optional()?;

    if let Some(price_id) = existing_id {
        transaction.execute(
            r#"
            UPDATE instrument_price
            SET price_text = ?1,
                currency_code = ?2,
                source = NULL,
                source_cell = NULL,
                origin = 'MANUAL'
            WHERE price_id = ?3
            "#,
            params![row.price_text, row.currency_code, price_id],
        )?;
    } else {
        transaction.execute(
            r#"
            INSERT INTO instrument_price (
                instrument_id,
                price_date,
                price_text,
                currency_code,
                origin
            ) VALUES (?1, ?2, ?3, ?4, 'MANUAL')
            "#,
            params![
                row.instrument_id,
                price_date,
                row.price_text,
                row.currency_code,
            ],
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(not(target_arch = "wasm32"))]
    use std::fs;

    #[cfg(not(target_arch = "wasm32"))]
    use rusqlite::{params, Connection};
    #[cfg(not(target_arch = "wasm32"))]
    use tempfile::tempdir;

    #[cfg(not(target_arch = "wasm32"))]
    use super::upsert_manual_prices_batch_with_connection;
    use super::{validate_batch_prices, BatchPriceInput, BatchPriceRowInput};
    #[cfg(not(target_arch = "wasm32"))]
    use crate::db::migration::migrate;
    use crate::error::AppError;

    fn sample_batch_input() -> BatchPriceInput {
        BatchPriceInput {
            price_date: "2099-07-09".to_string(),
            rows: vec![
                BatchPriceRowInput {
                    instrument_id: 1,
                    symbol: "AAA".to_string(),
                    instrument_name: "Alpha".to_string(),
                    currency_code: "NTD".to_string(),
                    price: "123.45".to_string(),
                },
                BatchPriceRowInput {
                    instrument_id: 2,
                    symbol: "BBB".to_string(),
                    instrument_name: "Beta".to_string(),
                    currency_code: "USD".to_string(),
                    price: "67.89".to_string(),
                },
            ],
        }
    }

    #[test]
    fn skips_blank_price_rows() {
        let mut input = sample_batch_input();
        input.rows[1].price = "   ".to_string();

        let validated = validate_batch_prices(&input).expect("valid batch prices");

        assert_eq!(validated.rows.len(), 1);
        assert_eq!(validated.rows[0].price, "123.45");
    }

    #[test]
    fn rejects_when_all_rows_are_blank() {
        let mut input = sample_batch_input();
        input.rows[0].price.clear();
        input.rows[1].price = " ".to_string();

        let error = validate_batch_prices(&input).expect_err("all blank rows should fail");
        assert!(matches!(error, AppError::Validation(message) if message == "沒有要儲存的價格"));
    }

    #[test]
    fn rejects_invalid_price_text() {
        let mut input = sample_batch_input();
        input.rows[0].price = "abc".to_string();

        let error = validate_batch_prices(&input).expect_err("invalid price should fail");
        assert!(
            matches!(error, AppError::Validation(message) if message.contains("AAA Alpha 的新市價格式錯誤"))
        );
    }

    #[test]
    fn rejects_non_positive_price() {
        let mut input = sample_batch_input();
        input.rows[0].price = "0".to_string();

        let error = validate_batch_prices(&input).expect_err("zero price should fail");
        assert!(
            matches!(error, AppError::Validation(message) if message.contains("AAA Alpha 的新市價必須大於 0"))
        );
    }

    #[test]
    fn rejects_duplicate_instrument_in_same_batch() {
        let mut input = sample_batch_input();
        input.rows[1].instrument_id = 1;

        let error = validate_batch_prices(&input).expect_err("duplicate instrument should fail");
        assert!(
            matches!(error, AppError::Validation(message) if message.contains("同一批次不可重複輸入"))
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn writes_multiple_manual_price_rows() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .expect("enable foreign keys");
        migrate(&mut connection).expect("migrate temp db");

        let instrument_rows: Vec<(i64, String, String, String)> = {
            let mut statement = connection
                .prepare(
                    r#"
                    SELECT instrument_id, symbol, name, trading_currency_code
                    FROM instrument
                    ORDER BY instrument_id
                    LIMIT 2
                    "#,
                )
                .expect("prepare instrument query");
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .expect("query instruments");
            rows.collect::<Result<Vec<_>, _>>()
                .expect("collect instruments")
        };

        let input = BatchPriceInput {
            price_date: "2099-07-09".to_string(),
            rows: instrument_rows
                .iter()
                .zip(["101.5", "202.75"])
                .map(
                    |((instrument_id, symbol, instrument_name, currency_code), price)| {
                        BatchPriceRowInput {
                            instrument_id: *instrument_id,
                            symbol: symbol.clone(),
                            instrument_name: instrument_name.clone(),
                            currency_code: currency_code.clone(),
                            price: price.to_string(),
                        }
                    },
                )
                .collect(),
        };

        let saved_count = upsert_manual_prices_batch_with_connection(&mut connection, input)
            .expect("batch save succeeds");

        assert_eq!(saved_count, 2);

        let inserted_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM instrument_price WHERE price_date = '2099-07-09' AND origin = 'MANUAL'",
                [],
                |row| row.get(0),
            )
            .expect("count inserted rows");
        assert_eq!(inserted_count, 2);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn updates_existing_manual_price_row_for_same_key() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .expect("enable foreign keys");
        migrate(&mut connection).expect("migrate temp db");

        let (instrument_id, symbol, instrument_name, currency_code): (i64, String, String, String) = connection
            .query_row(
                "SELECT instrument_id, symbol, name, trading_currency_code FROM instrument ORDER BY instrument_id LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("load instrument");

        let first_input = BatchPriceInput {
            price_date: "2099-07-10".to_string(),
            rows: vec![BatchPriceRowInput {
                instrument_id,
                symbol: symbol.clone(),
                instrument_name: instrument_name.clone(),
                currency_code: currency_code.clone(),
                price: "10".to_string(),
            }],
        };
        let second_input = BatchPriceInput {
            price_date: "2099-07-10".to_string(),
            rows: vec![BatchPriceRowInput {
                instrument_id,
                symbol,
                instrument_name,
                currency_code,
                price: "11.25".to_string(),
            }],
        };

        upsert_manual_prices_batch_with_connection(&mut connection, first_input)
            .expect("first save");
        upsert_manual_prices_batch_with_connection(&mut connection, second_input)
            .expect("second save");

        let (price_text, manual_count): (String, i64) = connection
            .query_row(
                r#"
                SELECT
                    (SELECT price_text FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-07-10' AND origin = 'MANUAL' ORDER BY price_id DESC LIMIT 1),
                    (SELECT COUNT(*) FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-07-10' AND origin = 'MANUAL')
                "#,
                params![instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("query updated price");

        assert_eq!(price_text, "11.25");
        assert_eq!(manual_count, 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn rolls_back_whole_batch_on_database_error() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .expect("enable foreign keys");
        migrate(&mut connection).expect("migrate temp db");

        let (instrument_id, symbol, instrument_name, currency_code): (i64, String, String, String) = connection
            .query_row(
                "SELECT instrument_id, symbol, name, trading_currency_code FROM instrument ORDER BY instrument_id LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("load instrument");

        let input = BatchPriceInput {
            price_date: "2099-07-11".to_string(),
            rows: vec![
                BatchPriceRowInput {
                    instrument_id,
                    symbol: symbol.clone(),
                    instrument_name: instrument_name.clone(),
                    currency_code: currency_code.clone(),
                    price: "10".to_string(),
                },
                BatchPriceRowInput {
                    instrument_id: 999_999,
                    symbol: "BAD".to_string(),
                    instrument_name: "Broken".to_string(),
                    currency_code,
                    price: "11".to_string(),
                },
            ],
        };

        let error = upsert_manual_prices_batch_with_connection(&mut connection, input)
            .expect_err("invalid instrument should fail");
        assert!(matches!(error, AppError::Database(_)));

        let inserted_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-07-11' AND origin = 'MANUAL'",
                params![instrument_id],
                |row| row.get(0),
            )
            .expect("count rolled back rows");
        assert_eq!(inserted_count, 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn preserves_excel_import_price_row_when_inserting_manual_batch_price() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .expect("enable foreign keys");
        migrate(&mut connection).expect("migrate temp db");

        let (instrument_id, symbol, instrument_name, currency_code): (i64, String, String, String) = connection
            .query_row(
                "SELECT instrument_id, symbol, name, trading_currency_code FROM instrument ORDER BY instrument_id LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("load instrument");

        connection
            .execute(
                r#"
                INSERT INTO instrument_price (
                    instrument_id,
                    price_date,
                    price_text,
                    currency_code,
                    source,
                    source_cell,
                    origin
                ) VALUES (?1, '2099-07-12', '88', ?2, 'sheet', 'A1', 'EXCEL_IMPORT')
                "#,
                params![instrument_id, currency_code],
            )
            .expect("insert import price row");

        let input = BatchPriceInput {
            price_date: "2099-07-12".to_string(),
            rows: vec![BatchPriceRowInput {
                instrument_id,
                symbol,
                instrument_name,
                currency_code,
                price: "99.5".to_string(),
            }],
        };

        upsert_manual_prices_batch_with_connection(&mut connection, input)
            .expect("manual batch save");

        let (import_origin, import_source_cell): (String, Option<String>) = connection
            .query_row(
                r#"
                SELECT origin, source_cell
                FROM instrument_price
                WHERE instrument_id = ?1
                  AND price_date = '2099-07-12'
                  AND origin = 'EXCEL_IMPORT'
                ORDER BY price_id DESC
                LIMIT 1
                "#,
                params![instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("import row preserved");
        assert_eq!(import_origin, "EXCEL_IMPORT");
        assert_eq!(import_source_cell.as_deref(), Some("A1"));

        let (manual_price_text, manual_count): (String, i64) = connection
            .query_row(
                r#"
                SELECT
                    (SELECT price_text FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-07-12' AND origin = 'MANUAL' ORDER BY price_id DESC LIMIT 1),
                    (SELECT COUNT(*) FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-07-12' AND origin = 'MANUAL')
                "#,
                params![instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("manual row inserted");
        assert_eq!(manual_price_text, "99.5");
        assert_eq!(manual_count, 1);
    }
}
