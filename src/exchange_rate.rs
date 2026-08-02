#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{params, Connection, OptionalExtension};

#[cfg(not(target_arch = "wasm32"))]
use crate::db::open_manual_write_database;
use crate::decimal::{normalize_decimal_text, parse_decimal_field};
use crate::error::{AppError, AppResult};

#[derive(Clone, Debug, PartialEq)]
pub struct ExchangeRateInput {
    pub base_currency_code: String,
    pub rate_date: String,
    pub rate: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq)]
struct ValidatedExchangeRateInput {
    base_currency_code: String,
    rate_date: String,
    rate_text: String,
    note: Option<String>,
}

#[allow(dead_code)]
pub fn validate_exchange_rate_input(input: &ExchangeRateInput) -> AppResult<ExchangeRateInput> {
    let validated = validate_exchange_rate_input_inner(input)?;

    Ok(ExchangeRateInput {
        base_currency_code: validated.base_currency_code,
        rate_date: validated.rate_date,
        rate: validated.rate_text,
        note: validated.note.unwrap_or_default(),
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub fn upsert_manual_exchange_rate(input: ExchangeRateInput) -> AppResult<()> {
    let mut connection = open_manual_write_database()?;
    upsert_manual_exchange_rate_with_connection(&mut connection, input)
}

#[cfg(target_arch = "wasm32")]
pub fn upsert_manual_exchange_rate(_input: ExchangeRateInput) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 匯率維護".to_string(),
    ))
}

fn validate_exchange_rate_input_inner(
    input: &ExchangeRateInput,
) -> AppResult<ValidatedExchangeRateInput> {
    let base_currency_code = input.base_currency_code.trim().to_string();
    if base_currency_code.is_empty() {
        return Err(AppError::Validation("請輸入來源幣別".to_string()));
    }
    if base_currency_code == "NTD" {
        return Err(AppError::Validation("來源幣別不可為 NTD".to_string()));
    }

    let rate_date = input.rate_date.trim().to_string();
    if rate_date.is_empty() {
        return Err(AppError::Validation("請輸入匯率日期".to_string()));
    }
    if !is_iso_date(&rate_date) {
        return Err(AppError::Validation(
            "匯率日期格式必須為 YYYY-MM-DD".to_string(),
        ));
    }

    let rate = parse_decimal_field("exchange_rate", &input.rate)?;
    if rate <= rust_decimal::Decimal::ZERO {
        return Err(AppError::Validation("匯率必須大於 0".to_string()));
    }

    Ok(ValidatedExchangeRateInput {
        base_currency_code,
        rate_date,
        rate_text: normalize_decimal_text(rate),
        note: normalize_optional_text(&input.note),
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

#[allow(dead_code)]
#[cfg(not(target_arch = "wasm32"))]
fn upsert_manual_exchange_rate_with_connection(
    connection: &mut Connection,
    input: ExchangeRateInput,
) -> AppResult<()> {
    let validated = validate_exchange_rate_input_inner(&input)?;
    let transaction = connection.transaction()?;

    ensure_currency_exists(&transaction, &validated.base_currency_code)?;

    let existing_id: Option<i64> = transaction
        .query_row(
            r#"
            SELECT exchange_rate_id
            FROM exchange_rate
            WHERE rate_date = ?1
              AND base_currency_code = ?2
              AND quote_currency_code = 'NTD'
              AND origin = 'MANUAL'
            ORDER BY exchange_rate_id DESC
            LIMIT 1
            "#,
            params![validated.rate_date, validated.base_currency_code],
            |row| row.get(0),
        )
        .optional()?;

    if let Some(exchange_rate_id) = existing_id {
        transaction.execute(
            r#"
            UPDATE exchange_rate
            SET rate_text = ?1,
                note = ?2,
                source_sheet = NULL,
                source_cell = NULL,
                origin = 'MANUAL'
            WHERE exchange_rate_id = ?3
            "#,
            params![validated.rate_text, validated.note, exchange_rate_id],
        )?;
    } else {
        transaction.execute(
            r#"
            INSERT INTO exchange_rate (
                rate_date,
                base_currency_code,
                quote_currency_code,
                rate_text,
                origin,
                note
            ) VALUES (?1, ?2, 'NTD', ?3, 'MANUAL', ?4)
            "#,
            params![
                validated.rate_date,
                validated.base_currency_code,
                validated.rate_text,
                validated.note,
            ],
        )?;
    }

    transaction.commit()?;
    Ok(())
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
    #[cfg(not(target_arch = "wasm32"))]
    use std::fs;

    #[cfg(not(target_arch = "wasm32"))]
    use rusqlite::Connection;
    #[cfg(not(target_arch = "wasm32"))]
    use tempfile::tempdir;

    #[cfg(not(target_arch = "wasm32"))]
    use super::upsert_manual_exchange_rate_with_connection;
    use super::{validate_exchange_rate_input, ExchangeRateInput};
    #[cfg(not(target_arch = "wasm32"))]
    use crate::error::AppError;

    fn sample_input() -> ExchangeRateInput {
        ExchangeRateInput {
            base_currency_code: "USD".to_string(),
            rate_date: "2099-07-15".to_string(),
            rate: "31.25".to_string(),
            note: "manual".to_string(),
        }
    }

    #[test]
    fn trims_blank_note_to_empty_string() {
        let input = ExchangeRateInput {
            note: "   ".to_string(),
            ..sample_input()
        };

        let validated = validate_exchange_rate_input(&input).expect("valid input");
        assert_eq!(validated.note, "");
    }

    #[test]
    fn rejects_ntd_as_base_currency() {
        let input = ExchangeRateInput {
            base_currency_code: "NTD".to_string(),
            ..sample_input()
        };

        let error = validate_exchange_rate_input(&input).expect_err("ntd base should fail");
        assert!(
            matches!(error, AppError::Validation(message) if message.contains("來源幣別不可為 NTD"))
        );
    }

    #[test]
    fn rejects_non_positive_rate() {
        let input = ExchangeRateInput {
            rate: "0".to_string(),
            ..sample_input()
        };

        let error = validate_exchange_rate_input(&input).expect_err("zero rate should fail");
        assert!(
            matches!(error, AppError::Validation(message) if message.contains("匯率必須大於 0"))
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn inserts_and_updates_manual_exchange_rate() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .expect("fk on");

        upsert_manual_exchange_rate_with_connection(&mut connection, sample_input())
            .expect("first insert");
        upsert_manual_exchange_rate_with_connection(
            &mut connection,
            ExchangeRateInput {
                rate: "31.5".to_string(),
                note: "updated".to_string(),
                ..sample_input()
            },
        )
        .expect("update manual row");

        let (rate_text, note, manual_count): (String, Option<String>, i64) = connection
            .query_row(
                r#"
                SELECT
                    (SELECT rate_text FROM exchange_rate WHERE base_currency_code = 'USD' AND quote_currency_code = 'NTD' AND rate_date = '2099-07-15' AND origin = 'MANUAL' LIMIT 1),
                    (SELECT note FROM exchange_rate WHERE base_currency_code = 'USD' AND quote_currency_code = 'NTD' AND rate_date = '2099-07-15' AND origin = 'MANUAL' LIMIT 1),
                    (SELECT COUNT(*) FROM exchange_rate WHERE base_currency_code = 'USD' AND quote_currency_code = 'NTD' AND rate_date = '2099-07-15' AND origin = 'MANUAL')
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("query manual row");

        assert_eq!(rate_text, "31.5");
        assert_eq!(note.as_deref(), Some("updated"));
        assert_eq!(manual_count, 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn preserves_import_exchange_rate_row() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .expect("fk on");

        connection.execute(
            r#"
            INSERT INTO exchange_rate (
                rate_date, base_currency_code, quote_currency_code, rate_text, origin, source_sheet, source_cell
            ) VALUES ('2099-07-16', 'USD', 'NTD', '30.8', 'EXCEL_IMPORT', 'sheet', 'B2')
            "#,
            [],
        ).expect("insert import row");

        upsert_manual_exchange_rate_with_connection(
            &mut connection,
            ExchangeRateInput {
                rate_date: "2099-07-16".to_string(),
                rate: "31".to_string(),
                note: "manual".to_string(),
                ..sample_input()
            },
        )
        .expect("manual save");

        let (import_origin, import_source_cell): (String, Option<String>) = connection
            .query_row(
                "SELECT origin, source_cell FROM exchange_rate WHERE rate_date = '2099-07-16' AND base_currency_code = 'USD' AND quote_currency_code = 'NTD' AND origin = 'EXCEL_IMPORT' LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("import row preserved");

        assert_eq!(import_origin, "EXCEL_IMPORT");
        assert_eq!(import_source_cell.as_deref(), Some("B2"));
    }
}
