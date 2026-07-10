use chrono::NaiveDate;
use rust_decimal::Decimal;

#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{params, Connection, OptionalExtension};

#[cfg(not(target_arch = "wasm32"))]
use crate::db::open_writable_database;
use crate::decimal::normalize_decimal_text;
use crate::error::{AppError, AppResult};

#[derive(Clone, Debug, PartialEq)]
pub struct AccountAssetInput {
    pub source_snapshot_id: Option<i64>,
    pub account_id: i64,
    pub snapshot_date: String,
    pub asset_type: String,
    pub currency_code: String,
    pub quantity: String,
    pub invested_amount: String,
    pub current_value_override: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedAccountAssetInput {
    pub account_id: i64,
    pub snapshot_date: NaiveDate,
    pub asset_type: String,
    pub currency_code: String,
    pub quantity: Option<Decimal>,
    pub invested_amount: Option<Decimal>,
    pub current_value_override: Option<Decimal>,
    pub note: Option<String>,
}

pub fn is_foreign_currency_asset(currency_code: &str) -> bool {
    currency_code != "NTD"
}

pub fn asset_type_label(asset_type: &str) -> &'static str {
    match asset_type {
        "DEMAND_DEPOSIT" => "活期存款",
        "TIME_DEPOSIT" => "定期存款",
        "FOREIGN_DEMAND_DEPOSIT" => "外幣活存",
        "FOREIGN_TIME_DEPOSIT" => "外幣定存",
        "BROKERAGE_CASH" => "證券戶現金",
        "SETTLEMENT_CASH" => "交割款",
        _ => "其他",
    }
}

pub fn validate_account_asset_input(
    input: &AccountAssetInput,
) -> AppResult<ValidatedAccountAssetInput> {
    let snapshot_date_str = input.snapshot_date.trim().to_string();
    if snapshot_date_str.is_empty() {
        return Err(AppError::Validation("請輸入資料日期".to_string()));
    }
    let snapshot_date = NaiveDate::parse_from_str(&snapshot_date_str, "%Y-%m-%d")
        .map_err(|_| AppError::Validation("資料日期格式必須為 YYYY-MM-DD".to_string()))?;

    let quantity = parse_optional_decimal("數量", &input.quantity)?;
    let invested_amount = parse_optional_decimal("投入金額", &input.invested_amount)?;
    let current_value_override = parse_optional_decimal("目前價值", &input.current_value_override)?;

    for (label, value) in [
        ("數量", &quantity),
        ("投入金額", &invested_amount),
        ("目前價值", &current_value_override),
    ] {
        if let Some(value) = value {
            if value.is_sign_negative() {
                return Err(AppError::Validation(format!("{label}不可小於 0")));
            }
        }
    }

    if quantity.is_none() && invested_amount.is_none() && current_value_override.is_none() {
        return Err(AppError::Validation(
            "數量、投入金額與目前價值至少需要填寫一項".to_string(),
        ));
    }

    let note = match input.note.trim() {
        "" => None,
        value => Some(value.to_string()),
    };

    Ok(ValidatedAccountAssetInput {
        account_id: input.account_id,
        snapshot_date,
        asset_type: input.asset_type.clone(),
        currency_code: input.currency_code.clone(),
        quantity,
        invested_amount,
        current_value_override,
        note,
    })
}

fn parse_optional_decimal(field: &'static str, value: &str) -> AppResult<Option<Decimal>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed
        .parse::<Decimal>()
        .map(Some)
        .map_err(|_| AppError::InvalidDecimal { field })
}

#[cfg(not(target_arch = "wasm32"))]
pub fn upsert_manual_account_asset(input: ValidatedAccountAssetInput) -> AppResult<i64> {
    let mut connection = open_writable_database()?;
    upsert_manual_account_asset_with_connection(&mut connection, &input)
}

#[cfg(not(target_arch = "wasm32"))]
fn upsert_manual_account_asset_with_connection(
    connection: &mut Connection,
    input: &ValidatedAccountAssetInput,
) -> AppResult<i64> {
    let transaction = connection.transaction()?;

    let snapshot_date_str = input.snapshot_date.format("%Y-%m-%d").to_string();

    let is_foreign = is_foreign_currency_asset(&input.currency_code);

    let quantity_text = input.quantity.map(normalize_decimal_text);
    let invested_amount_text = input.invested_amount.map(normalize_decimal_text);
    let current_value_override_text = if is_foreign {
        None
    } else {
        input.current_value_override.map(normalize_decimal_text)
    };

    let existing_id: Option<i64> = transaction
        .query_row(
            r#"
            SELECT snapshot_id
            FROM account_asset_snapshot
            WHERE account_id = ?1
              AND snapshot_date = ?2
              AND asset_type = ?3
              AND currency_code = ?4
              AND origin = 'MANUAL'
            LIMIT 1
            "#,
            params![
                input.account_id,
                snapshot_date_str,
                input.asset_type,
                input.currency_code,
            ],
            |row| row.get(0),
        )
        .optional()?;

    let snapshot_id = if let Some(snapshot_id) = existing_id {
        transaction.execute(
            r#"
            UPDATE account_asset_snapshot
            SET quantity_text = ?1,
                invested_amount_text = ?2,
                current_value_override_text = ?3,
                note = ?4,
                source_sheet = NULL,
                source_row = NULL
            WHERE snapshot_id = ?5
              AND origin = 'MANUAL'
            "#,
            params![
                quantity_text,
                invested_amount_text,
                current_value_override_text,
                input.note,
                snapshot_id,
            ],
        )?;
        snapshot_id
    } else {
        transaction.execute(
            r#"
            INSERT INTO account_asset_snapshot (
                account_id, snapshot_date, asset_type, currency_code,
                quantity_text, invested_amount_text, current_value_override_text,
                note, origin, source_sheet, source_row
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'MANUAL', NULL, NULL)
            "#,
            params![
                input.account_id,
                snapshot_date_str,
                input.asset_type,
                input.currency_code,
                quantity_text,
                invested_amount_text,
                current_value_override_text,
                input.note,
            ],
        )?;
        transaction.last_insert_rowid()
    };

    transaction.commit()?;
    Ok(snapshot_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_input() -> AccountAssetInput {
        AccountAssetInput {
            source_snapshot_id: Some(1),
            account_id: 1,
            snapshot_date: "2099-01-01".to_string(),
            asset_type: "DEMAND_DEPOSIT".to_string(),
            currency_code: "NTD".to_string(),
            quantity: "".to_string(),
            invested_amount: "".to_string(),
            current_value_override: "100000".to_string(),
            note: "測試".to_string(),
        }
    }

    #[test]
    fn rejects_all_blank_fields() {
        let input = AccountAssetInput {
            quantity: "".to_string(),
            invested_amount: "".to_string(),
            current_value_override: "".to_string(),
            ..sample_input()
        };
        let error = validate_account_asset_input(&input).expect_err("should fail");
        assert!(matches!(error, AppError::Validation(_)));
    }

    #[test]
    fn rejects_negative_quantity() {
        let input = AccountAssetInput {
            quantity: "-100".to_string(),
            ..sample_input()
        };
        let error = validate_account_asset_input(&input).expect_err("should fail");
        assert!(matches!(error, AppError::Validation(_)));
    }

    #[test]
    fn rejects_negative_current_value() {
        let input = AccountAssetInput {
            current_value_override: "-50".to_string(),
            ..sample_input()
        };
        let error = validate_account_asset_input(&input).expect_err("should fail");
        assert!(matches!(error, AppError::Validation(_)));
    }

    #[test]
    fn normalizes_decimal_input() {
        let input = AccountAssetInput {
            current_value_override: "00100.5000".to_string(),
            ..sample_input()
        };
        let validated = validate_account_asset_input(&input).expect("valid");
        let stored = validated
            .current_value_override
            .map(|v| normalize_decimal_text(v));
        assert_eq!(stored, Some("100.5".to_string()));
    }

    #[test]
    fn trims_blank_note_to_none() {
        let input = AccountAssetInput {
            note: "  ".to_string(),
            ..sample_input()
        };
        let validated = validate_account_asset_input(&input).expect("valid");
        assert_eq!(validated.note, None);
    }

    #[test]
    fn accepts_foreign_asset_with_only_quantity() {
        let input = AccountAssetInput {
            currency_code: "USD".to_string(),
            quantity: "10000".to_string(),
            current_value_override: "".to_string(),
            invested_amount: "".to_string(),
            ..sample_input()
        };
        let validated = validate_account_asset_input(&input).expect("valid");
        assert!(validated.quantity.is_some());
        assert!(validated.invested_amount.is_none());
        assert!(validated.current_value_override.is_none());
    }

    #[test]
    fn accepts_ntd_asset_with_only_override() {
        let input = AccountAssetInput {
            quantity: "".to_string(),
            invested_amount: "".to_string(),
            current_value_override: "150000".to_string(),
            ..sample_input()
        };
        let validated = validate_account_asset_input(&input).expect("valid");
        assert!(validated.quantity.is_none());
        assert!(validated.current_value_override.is_some());
    }

    #[test]
    fn rejects_invalid_date_format() {
        let input = AccountAssetInput {
            snapshot_date: "2099/01/01".to_string(),
            ..sample_input()
        };
        let error = validate_account_asset_input(&input).expect_err("should fail");
        assert!(matches!(error, AppError::Validation(_)));
    }

    #[test]
    fn rejects_empty_date() {
        let input = AccountAssetInput {
            snapshot_date: "".to_string(),
            ..sample_input()
        };
        let error = validate_account_asset_input(&input).expect_err("should fail");
        assert!(matches!(error, AppError::Validation(_)));
    }

    #[test]
    fn is_foreign_detects_non_twd() {
        assert!(is_foreign_currency_asset("USD"));
        assert!(is_foreign_currency_asset("JPY"));
        assert!(is_foreign_currency_asset("RMB"));
        assert!(!is_foreign_currency_asset("NTD"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    mod repo_tests {
        use std::fs;

        use rusqlite::Connection;
        use tempfile::tempdir;

        use super::*;
        use crate::db::migration::migrate;

        fn setup_db() -> (tempfile::TempDir, Connection) {
            let temp_dir = tempdir().expect("temp dir");
            let database_path = temp_dir.path().join("data.sqlite");
            fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");
            let mut connection = Connection::open(&database_path).expect("open temp db");
            migrate(&mut connection).expect("migrate temp db");
            (temp_dir, connection)
        }

        fn pick_ntd_asset(connection: &Connection) -> (i64, String, String) {
            connection
                .query_row(
                    r#"
                    SELECT account_id, asset_type, currency_code
                    FROM v_account_asset_value
                    WHERE currency_code = 'NTD'
                    LIMIT 1
                    "#,
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .expect("ntd asset row")
        }

        fn pick_foreign_asset(connection: &Connection) -> (i64, String, String) {
            connection
                .query_row(
                    r#"
                    SELECT account_id, asset_type, currency_code
                    FROM v_account_asset_value
                    WHERE currency_code != 'NTD'
                    LIMIT 1
                    "#,
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .expect("foreign asset row")
        }

        #[test]
        fn inserts_new_manual_snapshot() {
            let (_dir, mut connection) = setup_db();
            let (account_id, asset_type, currency_code) = pick_ntd_asset(&connection);

            let input = ValidatedAccountAssetInput {
                account_id,
                snapshot_date: NaiveDate::from_ymd_opt(2099, 6, 1).unwrap(),
                asset_type,
                currency_code,
                quantity: None,
                invested_amount: None,
                current_value_override: Some(Decimal::new(200000, 0)),
                note: Some("new snapshot".to_string()),
            };

            let snapshot_id = upsert_manual_account_asset_with_connection(&mut connection, &input)
                .expect("insert");

            let (origin, count): (String, i64) = connection
                .query_row(
                    r#"
                    SELECT origin, COUNT(*)
                    FROM account_asset_snapshot
                    WHERE snapshot_id = ?1
                    "#,
                    params![snapshot_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("query inserted");
            assert_eq!(origin, "MANUAL");
            assert_eq!(count, 1);
        }

        #[test]
        fn updates_existing_manual_snapshot() {
            let (_dir, mut connection) = setup_db();
            let (account_id, asset_type, currency_code) = pick_ntd_asset(&connection);

            let date = "2099-06-15";
            let mut input = ValidatedAccountAssetInput {
                account_id,
                snapshot_date: NaiveDate::from_ymd_opt(2099, 6, 15).unwrap(),
                asset_type: asset_type.clone(),
                currency_code: currency_code.clone(),
                quantity: None,
                invested_amount: None,
                current_value_override: Some(Decimal::new(100000, 0)),
                note: None,
            };

            upsert_manual_account_asset_with_connection(&mut connection, &input)
                .expect("first insert");

            input.current_value_override = Some(Decimal::new(150000, 0));

            upsert_manual_account_asset_with_connection(&mut connection, &input).expect("update");

            let count: i64 = connection
                .query_row(
                    r#"
                    SELECT COUNT(*)
                    FROM account_asset_snapshot
                    WHERE account_id = ?1
                      AND snapshot_date = ?2
                      AND asset_type = ?3
                      AND currency_code = ?4
                      AND origin = 'MANUAL'
                    "#,
                    params![account_id, date, asset_type, currency_code],
                    |row| row.get(0),
                )
                .expect("count");
            assert_eq!(count, 1, "should only have one row after upsert");
        }

        #[test]
        fn does_not_modify_excel_import_row() {
            let (_dir, mut connection) = setup_db();
            let (account_id, asset_type, currency_code) = pick_ntd_asset(&connection);

            let original_row: (i64, String) = connection
                .query_row(
                    r#"
                    SELECT snapshot_id, origin
                    FROM account_asset_snapshot
                    WHERE origin = 'EXCEL_IMPORT'
                      AND account_id = ?1 AND asset_type = ?2 AND currency_code = ?3
                    LIMIT 1
                    "#,
                    params![account_id, asset_type, currency_code],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("original excel import row");

            let input = ValidatedAccountAssetInput {
                account_id,
                snapshot_date: NaiveDate::from_ymd_opt(2099, 7, 1).unwrap(),
                asset_type: asset_type.clone(),
                currency_code: currency_code.clone(),
                quantity: None,
                invested_amount: None,
                current_value_override: Some(Decimal::new(999999, 0)),
                note: None,
            };

            upsert_manual_account_asset_with_connection(&mut connection, &input)
                .expect("insert new manual");

            let (still_exists, origin_unchanged): (i64, String) = connection
                .query_row(
                    r#"
                    SELECT COUNT(*), origin
                    FROM account_asset_snapshot
                    WHERE snapshot_id = ?1
                    "#,
                    params![original_row.0],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("original row unchanged");
            assert_eq!(still_exists, 1, "original excel import row should remain");
            assert_eq!(
                origin_unchanged, "EXCEL_IMPORT",
                "origin should remain unchanged"
            );
        }

        #[test]
        fn foreign_currency_saves_override_as_null() {
            let (_dir, mut connection) = setup_db();
            let (account_id, asset_type, currency_code) = pick_foreign_asset(&connection);

            let input = ValidatedAccountAssetInput {
                account_id,
                snapshot_date: NaiveDate::from_ymd_opt(2099, 8, 1).unwrap(),
                asset_type,
                currency_code,
                quantity: Some(Decimal::new(5000, 0)),
                invested_amount: None,
                current_value_override: Some(Decimal::new(999999, 0)),
                note: None,
            };

            upsert_manual_account_asset_with_connection(&mut connection, &input).expect("insert");

            let override_text: Option<String> = connection
                .query_row(
                    r#"
                    SELECT current_value_override_text
                    FROM account_asset_snapshot
                    WHERE account_id = ?1
                      AND snapshot_date = '2099-08-01'
                      AND origin = 'MANUAL'
                    "#,
                    params![account_id],
                    |row| row.get(0),
                )
                .expect("query");
            assert_eq!(override_text, None, "foreign override should be NULL");
        }
    }
}
