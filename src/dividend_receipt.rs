use rust_decimal::Decimal;

#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{params, Connection, OptionalExtension};

#[cfg(not(target_arch = "wasm32"))]
use crate::db::open_writable_database;
use crate::decimal::{normalize_decimal_text, parse_decimal_field};
use crate::error::{AppError, AppResult};

#[derive(Clone, Debug, PartialEq)]
pub struct DividendReceiptInput {
    pub account_id: i64,
    pub instrument_id: i64,
    pub received_on: String,
    pub net_amount_text: String,
    pub currency_code: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DividendReceiptUpdateInput {
    pub receipt_id: i64,
    pub account_id: i64,
    pub instrument_id: i64,
    pub received_on: String,
    pub net_amount_text: String,
    pub currency_code: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DividendReceiptDeleteInput {
    pub receipt_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
struct ValidatedDividendReceiptInput {
    account_id: i64,
    instrument_id: i64,
    received_on: String,
    net_amount_text: String,
    currency_code: String,
    note: Option<String>,
}

#[allow(dead_code)]
pub fn validate_dividend_receipt_input(
    input: &DividendReceiptInput,
) -> AppResult<DividendReceiptInput> {
    let validated = validate_dividend_receipt_input_inner(input)?;

    Ok(DividendReceiptInput {
        account_id: validated.account_id,
        instrument_id: validated.instrument_id,
        received_on: validated.received_on,
        net_amount_text: validated.net_amount_text,
        currency_code: validated.currency_code,
        note: validated.note.unwrap_or_default(),
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub fn insert_manual_dividend_receipt(input: DividendReceiptInput) -> AppResult<()> {
    let mut connection = open_writable_database()?;
    insert_manual_dividend_receipt_with_connection(&mut connection, input)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn update_manual_dividend_receipt(input: DividendReceiptUpdateInput) -> AppResult<()> {
    let mut connection = open_writable_database()?;
    update_manual_dividend_receipt_with_connection(&mut connection, input)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn delete_manual_dividend_receipt(input: DividendReceiptDeleteInput) -> AppResult<()> {
    let mut connection = open_writable_database()?;
    delete_manual_dividend_receipt_with_connection(&mut connection, input)
}

#[cfg(target_arch = "wasm32")]
pub fn insert_manual_dividend_receipt(_input: DividendReceiptInput) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 股息新增".to_string(),
    ))
}

#[cfg(target_arch = "wasm32")]
pub fn update_manual_dividend_receipt(_input: DividendReceiptUpdateInput) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 股息更新".to_string(),
    ))
}

#[cfg(target_arch = "wasm32")]
pub fn delete_manual_dividend_receipt(_input: DividendReceiptDeleteInput) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 股息刪除".to_string(),
    ))
}

fn validate_dividend_receipt_input_inner(
    input: &DividendReceiptInput,
) -> AppResult<ValidatedDividendReceiptInput> {
    validate_dividend_receipt_common(
        input.account_id,
        input.instrument_id,
        &input.received_on,
        &input.net_amount_text,
        &input.currency_code,
        &input.note,
    )
}

fn validate_dividend_receipt_update_inner(
    input: &DividendReceiptUpdateInput,
) -> AppResult<ValidatedDividendReceiptInput> {
    if input.receipt_id <= 0 {
        return Err(AppError::Validation("請選擇要更新的股息紀錄".to_string()));
    }

    validate_dividend_receipt_common(
        input.account_id,
        input.instrument_id,
        &input.received_on,
        &input.net_amount_text,
        &input.currency_code,
        &input.note,
    )
}

fn validate_dividend_receipt_common(
    account_id: i64,
    instrument_id: i64,
    received_on: &str,
    net_amount_text: &str,
    currency_code: &str,
    note: &str,
) -> AppResult<ValidatedDividendReceiptInput> {
    if account_id <= 0 {
        return Err(AppError::Validation("請選擇入帳帳戶".to_string()));
    }

    if instrument_id <= 0 {
        return Err(AppError::Validation("請選擇商品".to_string()));
    }

    let received_on = received_on.trim().to_string();
    if received_on.is_empty() {
        return Err(AppError::Validation("請輸入入帳日期".to_string()));
    }
    if !is_iso_date(&received_on) {
        return Err(AppError::Validation(
            "入帳日期格式必須為 YYYY-MM-DD".to_string(),
        ));
    }

    let currency_code = currency_code.trim().to_string();
    if currency_code.is_empty() {
        return Err(AppError::Validation("請選擇幣別".to_string()));
    }

    let net_amount_text = net_amount_text.trim();
    if net_amount_text.is_empty() {
        return Err(AppError::Validation("實收金額必須大於 0".to_string()));
    }

    let net_amount = parse_decimal_field("net_amount", net_amount_text)?;
    if net_amount <= Decimal::ZERO {
        return Err(AppError::Validation("實收金額必須大於 0".to_string()));
    }

    Ok(ValidatedDividendReceiptInput {
        account_id,
        instrument_id,
        received_on,
        net_amount_text: normalize_decimal_text(net_amount),
        currency_code,
        note: normalize_optional_text(note),
    })
}

fn normalize_optional_text(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
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

#[cfg(not(target_arch = "wasm32"))]
fn insert_manual_dividend_receipt_with_connection(
    connection: &mut Connection,
    input: DividendReceiptInput,
) -> AppResult<()> {
    let validated = validate_dividend_receipt_input_inner(&input)?;
    let transaction = connection.transaction()?;

    ensure_account_exists(&transaction, validated.account_id)?;
    ensure_instrument_exists(&transaction, validated.instrument_id)?;
    ensure_currency_exists(&transaction, &validated.currency_code)?;

    transaction.execute(
        r#"
        INSERT INTO dividend_receipt (
            account_id,
            instrument_id,
            received_on,
            gross_amount_text,
            tax_amount_text,
            fee_amount_text,
            net_amount_override_text,
            currency_code,
            note,
            origin
        ) VALUES (?1, ?2, ?3, NULL, '0', '0', ?4, ?5, ?6, 'MANUAL')
        "#,
        params![
            validated.account_id,
            validated.instrument_id,
            validated.received_on,
            validated.net_amount_text,
            validated.currency_code,
            validated.note,
        ],
    )?;

    transaction.commit()?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn update_manual_dividend_receipt_with_connection(
    connection: &mut Connection,
    input: DividendReceiptUpdateInput,
) -> AppResult<()> {
    let validated = validate_dividend_receipt_update_inner(&input)?;
    let transaction = connection.transaction()?;

    ensure_account_exists(&transaction, validated.account_id)?;
    ensure_instrument_exists(&transaction, validated.instrument_id)?;
    ensure_currency_exists(&transaction, &validated.currency_code)?;

    let updated = transaction.execute(
        r#"
        UPDATE dividend_receipt
        SET account_id = ?1,
            instrument_id = ?2,
            received_on = ?3,
            gross_amount_text = NULL,
            tax_amount_text = '0',
            fee_amount_text = '0',
            net_amount_override_text = ?4,
            currency_code = ?5,
            note = ?6
        WHERE receipt_id = ?7
          AND origin = 'MANUAL'
        "#,
        params![
            validated.account_id,
            validated.instrument_id,
            validated.received_on,
            validated.net_amount_text,
            validated.currency_code,
            validated.note,
            input.receipt_id,
        ],
    )?;

    if updated == 0 {
        return Err(AppError::Validation(
            "找不到可編輯的手動股息紀錄".to_string(),
        ));
    }

    transaction.commit()?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn delete_manual_dividend_receipt_with_connection(
    connection: &mut Connection,
    input: DividendReceiptDeleteInput,
) -> AppResult<()> {
    if input.receipt_id <= 0 {
        return Err(AppError::Validation("請選擇要刪除的股息紀錄".to_string()));
    }

    let transaction = connection.transaction()?;
    let deleted = transaction.execute(
        r#"
        DELETE FROM dividend_receipt
        WHERE receipt_id = ?1
          AND origin = 'MANUAL'
        "#,
        params![input.receipt_id],
    )?;

    if deleted == 0 {
        return Err(AppError::Validation(
            "找不到可刪除的手動股息紀錄".to_string(),
        ));
    }

    transaction.commit()?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_account_exists(connection: &Connection, account_id: i64) -> AppResult<()> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT account_id FROM account WHERE account_id = ?1 LIMIT 1",
            [account_id],
            |row| row.get(0),
        )
        .optional()?;

    if exists.is_some() {
        Ok(())
    } else {
        Err(AppError::Validation(format!("找不到帳戶：{account_id}")))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_instrument_exists(connection: &Connection, instrument_id: i64) -> AppResult<()> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT instrument_id FROM instrument WHERE instrument_id = ?1 LIMIT 1",
            [instrument_id],
            |row| row.get(0),
        )
        .optional()?;

    if exists.is_some() {
        Ok(())
    } else {
        Err(AppError::Validation(format!("找不到商品：{instrument_id}")))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_currency_exists(connection: &Connection, currency_code: &str) -> AppResult<()> {
    let exists: Option<String> = connection
        .query_row(
            "SELECT currency_code FROM currency WHERE currency_code = ?1 LIMIT 1",
            [currency_code],
            |row| row.get(0),
        )
        .optional()?;

    if exists.is_some() {
        Ok(())
    } else {
        Err(AppError::Validation(format!("找不到幣別：{currency_code}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_arch = "wasm32"))]
    use rusqlite::Connection;

    #[test]
    fn validate_dividend_receipt_input_rejects_blank_amount() {
        let input = DividendReceiptInput {
            account_id: 1,
            instrument_id: 1,
            received_on: "2026-07-09".to_string(),
            net_amount_text: "".to_string(),
            currency_code: "NTD".to_string(),
            note: String::new(),
        };

        let error = validate_dividend_receipt_input(&input).expect_err("should reject blank");
        assert!(error.to_string().contains("實收金額必須大於 0"));
    }

    #[test]
    fn validate_dividend_receipt_input_normalizes_values() {
        let input = DividendReceiptInput {
            account_id: 1,
            instrument_id: 1,
            received_on: " 2026-07-09 ".to_string(),
            net_amount_text: " 1000.5000 ".to_string(),
            currency_code: " ntd ".to_string(),
            note: "  note  ".to_string(),
        };

        let validated = validate_dividend_receipt_input(&input).expect("should validate");
        assert_eq!(validated.net_amount_text, "1000.5");
        assert_eq!(validated.currency_code, "ntd");
        assert_eq!(validated.note, "note");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn insert_manual_dividend_receipt_inserts_manual_row() {
        let mut connection = Connection::open_in_memory().expect("open db");
        connection
            .execute_batch(
                r#"
                CREATE TABLE account (
                    account_id INTEGER PRIMARY KEY,
                    display_name TEXT
                );

                CREATE TABLE instrument (
                    instrument_id INTEGER PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    trading_currency_code TEXT
                );

                CREATE TABLE currency (
                    currency_code TEXT PRIMARY KEY
                );

                CREATE TABLE dividend_receipt (
                    receipt_id INTEGER PRIMARY KEY,
                    account_id INTEGER NOT NULL,
                    instrument_id INTEGER NOT NULL,
                    received_on TEXT NOT NULL,
                    gross_amount_text TEXT,
                    tax_amount_text TEXT NOT NULL DEFAULT '0',
                    fee_amount_text TEXT NOT NULL DEFAULT '0',
                    net_amount_override_text TEXT,
                    currency_code TEXT NOT NULL,
                    note TEXT,
                    origin TEXT NOT NULL DEFAULT 'EXCEL_IMPORT'
                );

                INSERT INTO account (account_id, display_name) VALUES (1, 'Account 1');
                INSERT INTO instrument (instrument_id, symbol, name, trading_currency_code) VALUES (1, 'AAA', 'Alpha', 'NTD');
                INSERT INTO currency (currency_code) VALUES ('NTD');
                "#,
            )
            .expect("seed db");

        insert_manual_dividend_receipt_with_connection(
            &mut connection,
            DividendReceiptInput {
                account_id: 1,
                instrument_id: 1,
                received_on: "2026-07-09".to_string(),
                net_amount_text: "1000.50".to_string(),
                currency_code: "NTD".to_string(),
                note: "Monthly payout".to_string(),
            },
        )
        .expect("insert succeeds");

        let (count, origin, amount): (i64, String, String) = connection
            .query_row(
                r#"
                SELECT COUNT(*), origin, net_amount_override_text
                FROM dividend_receipt
                WHERE account_id = 1 AND instrument_id = 1 AND received_on = '2026-07-09'
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("fetch row");

        assert_eq!(count, 1);
        assert_eq!(origin, "MANUAL");
        assert_eq!(amount, "1000.5");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn insert_manual_dividend_receipt_allows_same_day_multiple_rows() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_dividend_receipt_db(&mut connection);

        for note in ["A", "B"] {
            insert_manual_dividend_receipt_with_connection(
                &mut connection,
                DividendReceiptInput {
                    account_id: 1,
                    instrument_id: 1,
                    received_on: "2026-07-09".to_string(),
                    net_amount_text: "1000".to_string(),
                    currency_code: "NTD".to_string(),
                    note: note.to_string(),
                },
            )
            .expect("insert succeeds");
        }

        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_receipt WHERE account_id = 1 AND instrument_id = 1 AND received_on = '2026-07-09'",
                [],
                |row| row.get(0),
            )
            .expect("count rows");

        assert_eq!(count, 2);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn update_manual_dividend_receipt_updates_manual_row() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_dividend_receipt_db(&mut connection);
        connection
            .execute(
                r#"
                INSERT INTO dividend_receipt (
                    receipt_id,
                    account_id,
                    instrument_id,
                    received_on,
                    gross_amount_text,
                    tax_amount_text,
                    fee_amount_text,
                    net_amount_override_text,
                    currency_code,
                    note,
                    origin
                ) VALUES (1, 1, 1, '2026-07-09', NULL, '0', '0', '1000', 'NTD', 'Initial', 'MANUAL')
                "#,
                [],
            )
            .expect("seed manual row");

        update_manual_dividend_receipt_with_connection(
            &mut connection,
            DividendReceiptUpdateInput {
                receipt_id: 1,
                account_id: 1,
                instrument_id: 1,
                received_on: "2026-07-10".to_string(),
                net_amount_text: "1200.00".to_string(),
                currency_code: "NTD".to_string(),
                note: "Updated".to_string(),
            },
        )
        .expect("update succeeds");

        let (received_on, amount, note): (String, String, String) = connection
            .query_row(
                "SELECT received_on, net_amount_override_text, note FROM dividend_receipt WHERE receipt_id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("read updated row");

        assert_eq!(received_on, "2026-07-10");
        assert_eq!(amount, "1200");
        assert_eq!(note, "Updated");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn delete_manual_dividend_receipt_removes_manual_row() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_dividend_receipt_db(&mut connection);
        connection
            .execute(
                r#"
                INSERT INTO dividend_receipt (
                    receipt_id,
                    account_id,
                    instrument_id,
                    received_on,
                    gross_amount_text,
                    tax_amount_text,
                    fee_amount_text,
                    net_amount_override_text,
                    currency_code,
                    note,
                    origin
                ) VALUES (1, 1, 1, '2026-07-09', NULL, '0', '0', '1000', 'NTD', 'Initial', 'MANUAL')
                "#,
                [],
            )
            .expect("seed manual row");

        delete_manual_dividend_receipt_with_connection(
            &mut connection,
            DividendReceiptDeleteInput { receipt_id: 1 },
        )
        .expect("delete succeeds");

        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM dividend_receipt", [], |row| {
                row.get(0)
            })
            .expect("count rows");

        assert_eq!(count, 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn update_manual_dividend_receipt_rejects_excel_import_row() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_dividend_receipt_db(&mut connection);
        connection
            .execute(
                r#"
                INSERT INTO dividend_receipt (
                    receipt_id,
                    account_id,
                    instrument_id,
                    received_on,
                    gross_amount_text,
                    tax_amount_text,
                    fee_amount_text,
                    net_amount_override_text,
                    currency_code,
                    note,
                    origin
                ) VALUES (2, 1, 1, '2026-07-09', NULL, '0', '0', '1000', 'NTD', 'Import', 'EXCEL_IMPORT')
                "#,
                [],
            )
            .expect("seed import row");

        let error = update_manual_dividend_receipt_with_connection(
            &mut connection,
            DividendReceiptUpdateInput {
                receipt_id: 2,
                account_id: 1,
                instrument_id: 1,
                received_on: "2026-07-10".to_string(),
                net_amount_text: "1200.00".to_string(),
                currency_code: "NTD".to_string(),
                note: "Updated".to_string(),
            },
        )
        .expect_err("import rows must be read-only");

        assert!(error.to_string().contains("找不到可編輯的手動股息紀錄"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn seed_dividend_receipt_db(connection: &mut Connection) {
        connection
            .execute_batch(
                r#"
                CREATE TABLE account (
                    account_id INTEGER PRIMARY KEY,
                    display_name TEXT
                );

                CREATE TABLE instrument (
                    instrument_id INTEGER PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    trading_currency_code TEXT
                );

                CREATE TABLE currency (
                    currency_code TEXT PRIMARY KEY
                );

                CREATE TABLE dividend_receipt (
                    receipt_id INTEGER PRIMARY KEY,
                    account_id INTEGER NOT NULL,
                    instrument_id INTEGER NOT NULL,
                    received_on TEXT NOT NULL,
                    gross_amount_text TEXT,
                    tax_amount_text TEXT NOT NULL DEFAULT '0',
                    fee_amount_text TEXT NOT NULL DEFAULT '0',
                    net_amount_override_text TEXT,
                    currency_code TEXT NOT NULL,
                    note TEXT,
                    origin TEXT NOT NULL DEFAULT 'EXCEL_IMPORT'
                );

                INSERT INTO account (account_id, display_name) VALUES (1, 'Account 1');
                INSERT INTO instrument (instrument_id, symbol, name, trading_currency_code) VALUES (1, 'AAA', 'Alpha', 'NTD');
                INSERT INTO currency (currency_code) VALUES ('NTD');
                "#,
            )
            .expect("seed db");
    }
}
