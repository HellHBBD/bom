use rusqlite::{params, Connection, ErrorCode, OptionalExtension, TransactionBehavior};
use rust_decimal::Decimal;

use crate::decimal::{normalize_decimal_text, parse_decimal_field};
use crate::error::{AppError, AppResult};

const LATEST_VERSION: i64 = 12;
const MANUAL_WRITES_SQL: &str = include_str!("migrations/001_manual_writes.sql");
const PRODUCT_LEVEL_MARKET_DATA_SQL: &str =
    include_str!("migrations/002_product_level_market_data.sql");
const EXCHANGE_RATE_MANUAL_ORIGIN_SQL: &str =
    include_str!("migrations/003_exchange_rate_manual_origin.sql");
const DIVIDEND_ASSUMPTION_ACCOUNT_SCOPE_SQL: &str =
    include_str!("migrations/004_dividend_assumption_account_scope.sql");
const DIVIDEND_ASSUMPTION_ACCOUNT_SCOPE_FALLBACK_SQL: &str =
    include_str!("migrations/004_dividend_assumption_account_scope_fallback.sql");
const UI_PREFERENCE_SQL: &str = include_str!("migrations/005_ui_preference.sql");
const INSTRUMENT_FEE_RATES_SQL: &str = include_str!("migrations/006_instrument_fee_rates.sql");
const FEE_AWARE_HOLDING_METRICS_SQL: &str =
    include_str!("migrations/007_fee_aware_holding_metrics.sql");
const HOLDING_SNAPSHOT_FEE_RATE_SQL: &str =
    include_str!("migrations/008_holding_snapshot_fee_rate.sql");
const VALIDATE_FEE_RATE_TEXT_SQL: &str = include_str!("migrations/009_validate_fee_rate_text.sql");
const REPAIR_FEE_METRIC_VIEWS_SQL: &str =
    include_str!("migrations/010_repair_fee_metric_views.sql");
const INSTRUMENT_SYMBOL_IDENTITY_SQL: &str =
    include_str!("migrations/011_instrument_symbol_identity.sql");
const PRODUCT_LEVEL_DIVIDEND_ASSUMPTION_SQL: &str =
    include_str!("migrations/012_product_level_dividend_assumption.sql");
const LEGACY_BUY_FEE_RATE: Decimal = Decimal::from_parts(1425, 0, 0, false, 6);
const DIVIDEND_RECEIPT_AMOUNT_VIEW_SQL: &str = r#"
CREATE VIEW IF NOT EXISTS v_dividend_receipt_amount AS
SELECT
    dr.receipt_id,
    dr.account_id,
    dr.instrument_id,
    COALESCE(dr.origin, 'EXCEL_IMPORT') AS origin,
    dr.received_on,
    dr.gross_amount_text,
    dr.tax_amount_text,
    dr.fee_amount_text,
    COALESCE(
        CAST(dr.net_amount_override_text AS REAL),
        CAST(COALESCE(dr.gross_amount_text, '0') AS REAL)
            - CAST(COALESCE(dr.tax_amount_text, '0') AS REAL)
            - CAST(COALESCE(dr.fee_amount_text, '0') AS REAL)
    ) AS net_amount,
    dr.currency_code,
    dr.note
FROM dividend_receipt dr;
"#;
const DIVIDEND_ASSUMPTION_ACCOUNT_SCOPE_REPAIR_SQL: &str = r#"
DROP VIEW IF EXISTS v_asset_total;
DROP VIEW IF EXISTS v_holding_metrics;
DROP VIEW IF EXISTS v_latest_dividend_assumption;
DROP INDEX IF EXISTS uq_manual_dividend_assumption;
DROP INDEX IF EXISTS idx_dividend_assumption_account_date;
DROP INDEX IF EXISTS idx_dividend_assumption_instrument_date;

CREATE INDEX idx_dividend_assumption_account_date
ON dividend_assumption(account_id, instrument_id, effective_date);

CREATE INDEX idx_dividend_assumption_instrument_date
ON dividend_assumption(instrument_id, effective_date);

CREATE UNIQUE INDEX uq_manual_dividend_assumption
ON dividend_assumption (
    account_id,
    instrument_id,
    effective_date
)
WHERE origin = 'MANUAL';

CREATE VIEW v_latest_dividend_assumption AS
SELECT *
FROM (
    SELECT
        da.*,
        ROW_NUMBER() OVER (
            PARTITION BY da.account_id, da.instrument_id
            ORDER BY da.effective_date DESC,
                CASE WHEN da.origin = 'MANUAL' THEN 0 ELSE 1 END,
                da.assumption_id DESC
        ) AS row_rank
    FROM dividend_assumption da
)
WHERE row_rank = 1;

CREATE VIEW v_holding_metrics AS
SELECT
    h.holding_snapshot_id,
    h.account_id,
    ao.person_id,
    p.display_name AS owner_name,
    h.instrument_id,
    i.symbol,
    i.name AS instrument_name,
    i.instrument_type,
    i.asset_class,
    i.region_type,
    i.trading_currency_code,
    h.snapshot_date,
    CAST(h.quantity_text AS REAL) AS quantity,
    CAST(h.average_cost_text AS REAL) AS average_cost,
    pr.price_date AS market_price_date,
    pr.currency_code AS market_price_currency_code,
    CAST(pr.price_text AS REAL) AS market_price,
    CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) AS total_cost,
    CAST(h.quantity_text AS REAL) * CAST(pr.price_text AS REAL) AS market_value,
    CAST(h.quantity_text AS REAL)
        * (CAST(pr.price_text AS REAL) - CAST(h.average_cost_text AS REAL))
        AS unrealized_profit,
    CASE
        WHEN CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) = 0 THEN NULL
        ELSE (
            CAST(h.quantity_text AS REAL)
            * (CAST(pr.price_text AS REAL) - CAST(h.average_cost_text AS REAL))
        ) / (
            CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL)
        )
    END AS unrealized_return_rate,
    da.effective_date AS dividend_effective_date,
    da.currency_code AS dividend_currency_code,
    CAST(da.estimated_annual_dividend_per_unit_text AS REAL)
        AS estimated_annual_dividend_per_unit,
    CAST(h.quantity_text AS REAL)
        * CAST(da.estimated_annual_dividend_per_unit_text AS REAL)
        AS estimated_annual_dividend,
    CASE
        WHEN CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) = 0 THEN NULL
        ELSE (
            CAST(h.quantity_text AS REAL)
            * CAST(da.estimated_annual_dividend_per_unit_text AS REAL)
        ) / (
            CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL)
        )
    END AS estimated_yield_on_cost,
    da.payments_per_year,
    CAST(da.latest_dividend_per_unit_text AS REAL) AS latest_dividend_per_unit
FROM v_latest_holding h
JOIN instrument i ON i.instrument_id = h.instrument_id
LEFT JOIN v_latest_instrument_price pr
       ON pr.instrument_id = h.instrument_id
LEFT JOIN v_latest_dividend_assumption da
       ON da.account_id = h.account_id
      AND da.instrument_id = h.instrument_id
LEFT JOIN account_owner ao ON ao.account_id = h.account_id
LEFT JOIN person p ON p.person_id = ao.person_id;

CREATE VIEW v_asset_total AS
SELECT
    p.person_id,
    p.display_name AS owner_name,
    'ACCOUNT_ASSET' AS source_type,
    av.account_id,
    NULL AS instrument_id,
    av.current_value_ntd AS value_ntd
FROM v_account_asset_value av
JOIN account_owner ao ON ao.account_id = av.account_id
JOIN person p ON p.person_id = ao.person_id

UNION ALL

SELECT
    hm.person_id,
    hm.owner_name,
    'HOLDING' AS source_type,
    hm.account_id,
    hm.instrument_id,
    hm.market_value AS value_ntd
FROM v_holding_metrics hm;
"#;
const ORIGIN_TABLES: [&str; 6] = [
    "account_asset_snapshot",
    "holding_snapshot",
    "instrument_price",
    "dividend_assumption",
    "dividend_receipt",
    "exchange_rate",
];
const SQLITE_CONSTRAINT_CHECK: i32 = 275;

pub fn latest_version() -> i64 {
    LATEST_VERSION
}

pub fn migrate(connection: &mut Connection) -> AppResult<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut version = current_version(&transaction)?;

    if version > LATEST_VERSION {
        return Err(AppError::Validation(format!(
            "資料庫版本 {version} 高於程式支援版本 {LATEST_VERSION}"
        )));
    }

    if version < 1 {
        for table_name in ORIGIN_TABLES {
            ensure_origin_column(&transaction, table_name)?;
        }
        transaction.execute_batch(MANUAL_WRITES_SQL)?;
        transaction.pragma_update(None, "user_version", 1_i64)?;
        version = 1;
    }

    if version < 2 {
        transaction.execute_batch(PRODUCT_LEVEL_MARKET_DATA_SQL)?;
        transaction.pragma_update(None, "user_version", 2_i64)?;
        version = 2;
    }

    if version < 3 {
        transaction.execute_batch(EXCHANGE_RATE_MANUAL_ORIGIN_SQL)?;
        transaction.pragma_update(None, "user_version", 3_i64)?;
        version = 3;
    }

    if version < 4 {
        migrate_v4_dividend_assumption_account_scope(&transaction)?;
        transaction.pragma_update(None, "user_version", 4_i64)?;
        version = 4;
    }

    if version < 5 {
        transaction.execute_batch(UI_PREFERENCE_SQL)?;
        transaction.pragma_update(None, "user_version", 5_i64)?;
        version = 5;
    }

    if version < 6 {
        let fee_rate_column_count = ["buy_fee_rate", "sell_fee_rate", "sell_transaction_tax_rate"]
            .iter()
            .map(|column_name| column_exists(&transaction, "instrument", column_name))
            .collect::<AppResult<Vec<_>>>()?
            .into_iter()
            .filter(|exists| *exists)
            .count();
        match fee_rate_column_count {
            0 => transaction.execute_batch(INSTRUMENT_FEE_RATES_SQL)?,
            3 => {}
            _ => {
                return Err(AppError::Validation(
                    "資料表 instrument 的費率欄位不完整".to_string(),
                ));
            }
        }
        validate_fee_rate_schema(&transaction)?;
        transaction.pragma_update(None, "user_version", 6_i64)?;
        version = 6;
    }

    if version < 7 {
        backfill_fee_inclusive_average_costs(&transaction)?;
        transaction.execute_batch(FEE_AWARE_HOLDING_METRICS_SQL)?;
        transaction.pragma_update(None, "user_version", 7_i64)?;
        version = 7;
    }

    if version < 8 {
        if !column_exists(&transaction, "holding_snapshot", "applied_buy_fee_rate")? {
            transaction.execute_batch(HOLDING_SNAPSHOT_FEE_RATE_SQL)?;
        }
        transaction.pragma_update(None, "user_version", 8_i64)?;
        version = 8;
    }

    if version < 9 {
        transaction.execute_batch(VALIDATE_FEE_RATE_TEXT_SQL)?;
        transaction.pragma_update(None, "user_version", 9_i64)?;
        version = 9;
    }

    if version < 10 {
        transaction.execute_batch(REPAIR_FEE_METRIC_VIEWS_SQL)?;
        transaction.pragma_update(None, "user_version", 10_i64)?;
        version = 10;
    }

    if version < 11 {
        transaction.execute_batch(INSTRUMENT_SYMBOL_IDENTITY_SQL)?;
        transaction.pragma_update(None, "user_version", 11_i64)?;
        version = 11;
    }

    if version < 12 {
        transaction.execute_batch(PRODUCT_LEVEL_DIVIDEND_ASSUMPTION_SQL)?;
        transaction.pragma_update(None, "user_version", 12_i64)?;
    }

    validate_manual_write_schema(&transaction)?;
    validate_ui_preference_schema(&transaction)?;
    validate_fee_rate_schema(&transaction)?;
    validate_holding_snapshot_fee_rate_schema(&transaction)?;
    validate_fee_rate_text_triggers(&transaction)?;
    validate_fee_metric_view(&transaction)?;
    validate_instrument_identity_schema(&transaction)?;

    transaction.commit()?;

    Ok(())
}

fn validate_instrument_identity_schema(connection: &Connection) -> AppResult<()> {
    let index_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'uq_instrument_symbol'",
        [],
        |row| row.get(0),
    )?;
    let normalized = normalize_schema_sql(&index_sql);
    if !(normalized.contains("UNIQUEINDEXUQ_INSTRUMENT_SYMBOLONINSTRUMENT(UPPER(TRIM(SYMBOL)))")
        || normalized.contains(
            "UNIQUEINDEXIFNOTEXISTSUQ_INSTRUMENT_SYMBOLONINSTRUMENT(UPPER(TRIM(SYMBOL)))",
        ))
        || !normalized.contains("WHERENULLIF(TRIM(SYMBOL),'')ISNOTNULL")
    {
        return Err(AppError::Validation(
            "商品代號唯一索引定義不正確".to_string(),
        ));
    }
    Ok(())
}

fn validate_fee_rate_text_triggers(connection: &Connection) -> AppResult<()> {
    for trigger_name in [
        "validate_instrument_fee_rates_insert",
        "validate_instrument_fee_rates_update",
        "validate_holding_snapshot_buy_fee_rate",
        "validate_holding_snapshot_buy_fee_rate_update",
    ] {
        let trigger_sql = connection.query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
            [trigger_name],
            |row| row.get::<_, String>(0),
        );
        let valid_trigger = trigger_sql
            .ok()
            .is_some_and(|sql| normalize_schema_sql(&sql).contains("GLOB"));
        if !valid_trigger {
            return Err(AppError::Validation(format!(
                "缺少商品費率格式驗證 trigger {trigger_name}"
            )));
        }
    }
    Ok(())
}

fn validate_fee_metric_view(connection: &Connection) -> AppResult<()> {
    let view_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = 'v_holding_metrics'",
        [],
        |row| row.get(0),
    )?;
    if !normalize_schema_sql(&view_sql).contains("H.APPLIED_BUY_FEE_RATE") {
        return Err(AppError::Validation(
            "持股指標 view 未使用快照買入手續費率".to_string(),
        ));
    }
    Ok(())
}

fn validate_holding_snapshot_fee_rate_schema(connection: &Connection) -> AppResult<()> {
    let column = connection
        .prepare("PRAGMA table_info(holding_snapshot)")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|(name, _, _, _)| name == "applied_buy_fee_rate");
    let Some((_, column_type, not_null, default_value)) = column else {
        return Err(AppError::Validation(
            "資料表 holding_snapshot 缺少套用買入手續費率欄位".to_string(),
        ));
    };
    if !column_type.eq_ignore_ascii_case("TEXT")
        || not_null != 1
        || default_value
            .as_deref()
            .map(|value| value.trim_matches('\'').trim_matches('"'))
            != Some("0.001425")
    {
        return Err(AppError::Validation(
            "資料表 holding_snapshot 的套用買入手續費率欄位定義不正確".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_fee_rates(
    buy_fee_rate: Decimal,
    sell_fee_rate: Decimal,
    sell_transaction_tax_rate: Decimal,
) -> AppResult<()> {
    let zero = Decimal::ZERO;
    let one = Decimal::ONE;
    if buy_fee_rate < zero || buy_fee_rate >= one {
        return Err(AppError::Validation(
            "買入手續費率必須介於 0 與 1 之間".to_string(),
        ));
    }
    if sell_fee_rate < zero || sell_fee_rate >= one {
        return Err(AppError::Validation(
            "賣出手續費率必須介於 0 與 1 之間".to_string(),
        ));
    }
    if sell_transaction_tax_rate < zero || sell_transaction_tax_rate >= one {
        return Err(AppError::Validation(
            "賣出交易稅率必須介於 0 與 1 之間".to_string(),
        ));
    }
    if sell_fee_rate + sell_transaction_tax_rate >= one {
        return Err(AppError::Validation(
            "賣出手續費率與交易稅率合計必須小於 1".to_string(),
        ));
    }
    Ok(())
}

fn validate_fee_rate_schema(connection: &Connection) -> AppResult<()> {
    let columns = connection
        .prepare("PRAGMA table_info(instrument)")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for (column_name, default_value) in [
        ("buy_fee_rate", "0.001425"),
        ("sell_fee_rate", "0"),
        ("sell_transaction_tax_rate", "0"),
    ] {
        let valid_column = columns
            .iter()
            .any(|(name, column_type, not_null, default)| {
                name == column_name
                    && column_type.eq_ignore_ascii_case("TEXT")
                    && *not_null == 1
                    && default
                        .as_deref()
                        .map(|value| value.trim_matches('\'').trim_matches('"'))
                        == Some(default_value)
            });
        if !valid_column {
            return Err(AppError::Validation(format!(
                "資料表 instrument 的費率欄位 {column_name} 定義不正確"
            )));
        }
    }
    let table_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'instrument'",
        [],
        |row| row.get(0),
    )?;
    let normalized_schema = normalize_schema_sql(&table_sql);
    for constraint in [
        "CHECK(CAST(BUY_FEE_RATEASREAL)>=0ANDCAST(BUY_FEE_RATEASREAL)<1)",
        "CHECK(CAST(SELL_FEE_RATEASREAL)>=0ANDCAST(SELL_FEE_RATEASREAL)<1)",
        "CHECK(CAST(SELL_TRANSACTION_TAX_RATEASREAL)>=0ANDCAST(SELL_TRANSACTION_TAX_RATEASREAL)<1ANDCAST(SELL_FEE_RATEASREAL)+CAST(SELL_TRANSACTION_TAX_RATEASREAL)<1)",
    ] {
        if !normalized_schema.contains(constraint) {
            return Err(AppError::Validation(
                "資料表 instrument 的費率限制不正確".to_string(),
            ));
        }
    }

    let mut statement = connection.prepare(
        "SELECT instrument_id, buy_fee_rate, sell_fee_rate, sell_transaction_tax_rate FROM instrument",
    )?;
    let rates = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    for rate in rates {
        let (instrument_id, buy_fee_rate, sell_fee_rate, sell_transaction_tax_rate) = rate?;
        let buy_fee_rate = parse_decimal_field("buy_fee_rate", &buy_fee_rate).map_err(|_| {
            AppError::Validation(format!("商品 {instrument_id} 的買入手續費率格式錯誤"))
        })?;
        let sell_fee_rate = parse_decimal_field("sell_fee_rate", &sell_fee_rate).map_err(|_| {
            AppError::Validation(format!("商品 {instrument_id} 的賣出手續費率格式錯誤"))
        })?;
        let sell_transaction_tax_rate =
            parse_decimal_field("sell_transaction_tax_rate", &sell_transaction_tax_rate).map_err(
                |_| AppError::Validation(format!("商品 {instrument_id} 的賣出交易稅率格式錯誤")),
            )?;
        validate_fee_rates(buy_fee_rate, sell_fee_rate, sell_transaction_tax_rate)?;
    }
    Ok(())
}

fn backfill_fee_inclusive_average_costs(connection: &Connection) -> AppResult<()> {
    let mut statement = connection.prepare(
        "SELECT holding_snapshot_id, account_id, instrument_id, snapshot_date, average_cost_text FROM holding_snapshot",
    )?;
    let snapshots = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
        ))
    })?;
    let snapshots = snapshots.collect::<Result<Vec<_>, _>>()?;
    for (snapshot_id, account_id, instrument_id, snapshot_date, average_cost_text) in snapshots {
        let context = format!(
            "持股快照 {snapshot_id}（帳戶 {account_id}、商品 {instrument_id}、日期 {snapshot_date}）"
        );
        let average_cost_text = average_cost_text
            .ok_or_else(|| AppError::Validation(format!("{context} 的平均成本不可為空白")))?;
        let average_cost = parse_decimal_field("average_cost", &average_cost_text)
            .map_err(|_| AppError::Validation(format!("{context} 的平均成本格式錯誤")))?;
        if average_cost.is_sign_negative() {
            return Err(AppError::Validation(format!(
                "{context} 的平均成本不可為負數"
            )));
        }
        connection.execute(
            "UPDATE holding_snapshot SET average_cost_text = ?1 WHERE holding_snapshot_id = ?2",
            params![
                normalize_decimal_text(average_cost * (Decimal::ONE + LEGACY_BUY_FEE_RATE)),
                snapshot_id
            ],
        )?;
    }
    Ok(())
}

pub(crate) fn validate_ui_preference_schema(connection: &Connection) -> AppResult<()> {
    let table_sql = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'ui_preference'",
            [],
            |row| row.get::<_, String>(0),
        )
        .map_err(|_| {
            AppError::Validation("資料表 ui_preference 不存在或無法讀取 schema".to_string())
        })?;
    let expected_sql = "CREATE TABLE ui_preference (preference_key TEXT PRIMARY KEY NOT NULL, value_text TEXT NOT NULL)";
    if normalize_schema_sql(&table_sql) != normalize_schema_sql(expected_sql) {
        return Err(AppError::Validation(
            "資料表 ui_preference 的欄位定義不符合偏好設定 schema".to_string(),
        ));
    }

    let columns = connection
        .prepare("PRAGMA table_info(ui_preference)")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let preference_key_valid = columns.iter().any(|(name, ty, not_null, primary_key)| {
        name == "preference_key"
            && ty.eq_ignore_ascii_case("TEXT")
            && *not_null == 1
            && *primary_key == 1
    });
    let value_text_valid = columns.iter().any(|(name, ty, not_null, _)| {
        name == "value_text" && ty.eq_ignore_ascii_case("TEXT") && *not_null == 1
    });
    let primary_key_column_count = columns
        .iter()
        .filter(|(_, _, _, primary_key)| *primary_key > 0)
        .count();
    if columns.len() != 2
        || !preference_key_valid
        || !value_text_valid
        || primary_key_column_count != 1
    {
        return Err(AppError::Validation(
            "資料表 ui_preference 的欄位定義不符合偏好設定 schema".to_string(),
        ));
    }

    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_whitespace() && *character != ';')
        .collect::<String>()
        .to_ascii_uppercase()
}

fn migrate_v4_dividend_assumption_account_scope(connection: &Connection) -> AppResult<()> {
    let already_account_scoped = column_exists(connection, "dividend_assumption", "account_id")?;
    if already_account_scoped {
        connection.execute_batch(DIVIDEND_ASSUMPTION_ACCOUNT_SCOPE_REPAIR_SQL)?;
        return Ok(());
    }

    if table_exists(connection, "dividend_assumption_account_archive")? {
        connection.execute_batch(DIVIDEND_ASSUMPTION_ACCOUNT_SCOPE_SQL)?;
        return Ok(());
    }

    connection.execute_batch(DIVIDEND_ASSUMPTION_ACCOUNT_SCOPE_FALLBACK_SQL)?;
    Ok(())
}

fn validate_manual_write_schema(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(DIVIDEND_RECEIPT_AMOUNT_VIEW_SQL)?;

    for table_name in ORIGIN_TABLES {
        validate_origin_definition(connection, table_name)?;
    }

    for index in manual_index_specs() {
        validate_manual_index(connection, &index)?;
    }

    validate_manual_write_behavior(connection)?;

    Ok(())
}

fn validate_manual_write_behavior(connection: &Connection) -> AppResult<()> {
    connection.execute_batch("SAVEPOINT manual_write_schema_validation")?;
    let validation_result = validate_manual_write_behavior_inner(connection);
    let cleanup_result = connection.execute_batch(
        "ROLLBACK TO manual_write_schema_validation; RELEASE manual_write_schema_validation;",
    );

    cleanup_result?;
    validation_result
}

fn validate_manual_write_behavior_inner(connection: &Connection) -> AppResult<()> {
    let Some(refs) = validation_refs(connection)? else {
        return Ok(());
    };
    let probe = validation_probe_prefix();
    let invalid_origin_date = format!("{probe}-invalid-origin");
    let asset_date = format!("{probe}-asset");
    let holding_date = format!("{probe}-holding");
    let assumption_date = format!("{probe}-assumption");
    let price_date = format!("{probe}-price");
    let exchange_rate_date = format!("{probe}-exchange-rate");

    expect_check_constraint(
        connection.execute(
            r#"
            INSERT INTO holding_snapshot (
                account_id,
                instrument_id,
                snapshot_date,
                quantity_text,
                average_cost_text,
                cost_currency_code,
                origin
            ) VALUES (?1, ?2, ?3, '1', '1', ?4, 'INVALID')
            "#,
            params![
                refs.account_id,
                refs.instrument_id,
                invalid_origin_date,
                refs.currency_code
            ],
        ),
        "holding_snapshot 必須拒絕無效 origin",
    )?;

    validate_duplicate_rejection(
        connection,
        "account_asset_snapshot 手動唯一索引未生效",
        |connection| {
            connection.execute(
                r#"
                INSERT INTO account_asset_snapshot (
                    account_id,
                    snapshot_date,
                    asset_type,
                    currency_code,
                    current_value_override_text,
                    origin
                ) VALUES (?1, ?2, 'OTHER', ?3, '1', 'MANUAL')
                "#,
                params![refs.account_id, asset_date, refs.currency_code],
            )
        },
    )?;

    validate_duplicate_rejection(
        connection,
        "holding_snapshot 手動唯一索引未生效",
        |connection| {
            connection.execute(
                r#"
                INSERT INTO holding_snapshot (
                    account_id,
                    instrument_id,
                    snapshot_date,
                    quantity_text,
                    average_cost_text,
                    cost_currency_code,
                    origin
                ) VALUES (?1, ?2, ?3, '1', '1', ?4, 'MANUAL')
                "#,
                params![
                    refs.account_id,
                    refs.instrument_id,
                    holding_date,
                    refs.currency_code
                ],
            )
        },
    )?;

    validate_duplicate_rejection(
        connection,
        "dividend_assumption 手動唯一索引未生效",
        |connection| {
            connection.execute(
                r#"
                INSERT INTO dividend_assumption (
                    instrument_id,
                    effective_date,
                    estimated_annual_dividend_per_unit_text,
                    currency_code,
                    origin
                ) VALUES (?1, ?2, '1', ?3, 'MANUAL')
                "#,
                params![refs.instrument_id, assumption_date, refs.currency_code],
            )
        },
    )?;

    validate_duplicate_rejection(
        connection,
        "instrument_price 手動唯一索引未生效",
        |connection| {
            connection.execute(
                r#"
                INSERT INTO instrument_price (
                    instrument_id,
                    price_date,
                    price_text,
                    currency_code,
                    origin
                ) VALUES (?1, ?2, '1', ?3, 'MANUAL')
                "#,
                params![refs.instrument_id, price_date, refs.currency_code],
            )
        },
    )?;

    if let (Some(base_currency_code), Some(quote_currency_code)) = (
        refs.exchange_rate_base_currency_code.as_deref(),
        refs.exchange_rate_quote_currency_code.as_deref(),
    ) {
        validate_duplicate_rejection(
            connection,
            "exchange_rate 手動唯一索引未生效",
            |connection| {
                connection.execute(
                    r#"
                INSERT INTO exchange_rate (
                    rate_date,
                    base_currency_code,
                    quote_currency_code,
                    rate_text,
                    origin
                ) VALUES (?1, ?2, ?3, '30', 'MANUAL')
                "#,
                    params![exchange_rate_date, base_currency_code, quote_currency_code],
                )
            },
        )?;
    }

    Ok(())
}

#[derive(Clone)]
struct ValidationRefs {
    account_id: i64,
    instrument_id: i64,
    currency_code: String,
    exchange_rate_base_currency_code: Option<String>,
    exchange_rate_quote_currency_code: Option<String>,
}

fn validation_refs(connection: &Connection) -> AppResult<Option<ValidationRefs>> {
    let account_id = validation_i64_ref(connection, "account", "account_id")?;
    let instrument_id = validation_i64_ref(connection, "instrument", "instrument_id")?;
    let currency_code = validation_string_ref(connection, "currency", "currency_code")?;
    let exchange_rate_quote_currency_code = validation_ntd_currency_ref(connection)?;
    let exchange_rate_base_currency_code =
        validation_non_ntd_currency_ref(connection, &exchange_rate_quote_currency_code)?;

    if account_id.is_empty_parent && instrument_id.is_empty_parent && currency_code.is_empty_parent
    {
        return Ok(None);
    }

    let account_id = account_id.value.ok_or_else(|| {
        AppError::Validation("資料表 account 沒有可供 migration 驗證的資料".to_string())
    })?;
    let instrument_id = instrument_id.value.ok_or_else(|| {
        AppError::Validation("資料表 instrument 沒有可供 migration 驗證的資料".to_string())
    })?;
    let currency_code = currency_code.value.ok_or_else(|| {
        AppError::Validation("資料表 currency 沒有可供 migration 驗證的資料".to_string())
    })?;
    Ok(Some(ValidationRefs {
        account_id,
        instrument_id,
        currency_code,
        exchange_rate_base_currency_code: exchange_rate_base_currency_code.value,
        exchange_rate_quote_currency_code: exchange_rate_quote_currency_code.value,
    }))
}

struct ValidationRef<T> {
    value: Option<T>,
    is_empty_parent: bool,
}

fn validation_i64_ref(
    connection: &Connection,
    table_name: &str,
    column_name: &str,
) -> AppResult<ValidationRef<i64>> {
    if !table_exists(connection, table_name)? {
        return Err(AppError::Validation(format!(
            "資料表 {table_name} 不存在，無法進行 migration 驗證"
        )));
    }

    let value = connection
        .query_row(
            &format!("SELECT {column_name} FROM {table_name} ORDER BY {column_name} LIMIT 1"),
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(ValidationRef {
        is_empty_parent: value.is_none(),
        value,
    })
}

fn validation_string_ref(
    connection: &Connection,
    table_name: &str,
    column_name: &str,
) -> AppResult<ValidationRef<String>> {
    if !table_exists(connection, table_name)? {
        return Err(AppError::Validation(format!(
            "資料表 {table_name} 不存在，無法進行 migration 驗證"
        )));
    }

    let value = connection
        .query_row(
            &format!("SELECT {column_name} FROM {table_name} ORDER BY {column_name} LIMIT 1"),
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(ValidationRef {
        is_empty_parent: value.is_none(),
        value,
    })
}

fn validation_ntd_currency_ref(connection: &Connection) -> AppResult<ValidationRef<String>> {
    if !table_exists(connection, "currency")? {
        return Err(AppError::Validation(
            "資料表 currency 不存在，無法進行 migration 驗證".to_string(),
        ));
    }

    let value = connection
        .query_row(
            "SELECT currency_code FROM currency WHERE currency_code = 'NTD' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(ValidationRef {
        is_empty_parent: value.is_none(),
        value,
    })
}

fn validation_non_ntd_currency_ref(
    connection: &Connection,
    quote_currency_code: &ValidationRef<String>,
) -> AppResult<ValidationRef<String>> {
    if !table_exists(connection, "currency")? {
        return Err(AppError::Validation(
            "資料表 currency 不存在，無法進行 migration 驗證".to_string(),
        ));
    }

    let excluded = quote_currency_code.value.as_deref().unwrap_or("NTD");
    let value = connection
        .query_row(
            "SELECT currency_code FROM currency WHERE currency_code <> ?1 ORDER BY currency_code LIMIT 1",
            [excluded],
            |row| row.get(0),
        )
        .optional()?;
    Ok(ValidationRef {
        is_empty_parent: value.is_none(),
        value,
    })
}

fn validation_probe_prefix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("__migration_validation_{}_{}", std::process::id(), nanos)
}

fn table_exists(connection: &Connection, table_name: &str) -> AppResult<bool> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table_name],
        |row| row.get(0),
    )?;
    Ok(count == 1)
}

fn validate_duplicate_rejection<F>(
    connection: &Connection,
    message: &'static str,
    insert: F,
) -> AppResult<()>
where
    F: Fn(&Connection) -> rusqlite::Result<usize>,
{
    insert(connection)?;
    expect_constraint(insert(connection), message)
}

fn expect_constraint(result: rusqlite::Result<usize>, message: &'static str) -> AppResult<()> {
    match result {
        Ok(_) => Err(AppError::Validation(message.to_string())),
        Err(error) if error.sqlite_error_code() == Some(ErrorCode::ConstraintViolation) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn expect_check_constraint(
    result: rusqlite::Result<usize>,
    message: &'static str,
) -> AppResult<()> {
    match result {
        Ok(_) => Err(AppError::Validation(message.to_string())),
        Err(rusqlite::Error::SqliteFailure(error, _))
            if error.extended_code == SQLITE_CONSTRAINT_CHECK =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

struct ManualIndexSpec {
    name: &'static str,
    table: &'static str,
    columns: &'static [&'static str],
    predicates: &'static [&'static str],
}

fn manual_index_specs() -> [ManualIndexSpec; 5] {
    [
        ManualIndexSpec {
            name: "uq_manual_asset_snapshot",
            table: "account_asset_snapshot",
            columns: &["account_id", "snapshot_date", "asset_type", "currency_code"],
            predicates: &["ORIGIN = 'MANUAL'"],
        },
        ManualIndexSpec {
            name: "uq_manual_holding_snapshot",
            table: "holding_snapshot",
            columns: &["account_id", "instrument_id", "snapshot_date"],
            predicates: &["ORIGIN = 'MANUAL'"],
        },
        ManualIndexSpec {
            name: "uq_manual_instrument_price",
            table: "instrument_price",
            columns: &["instrument_id", "price_date"],
            predicates: &["ORIGIN = 'MANUAL'"],
        },
        ManualIndexSpec {
            name: "uq_manual_dividend_assumption",
            table: "dividend_assumption",
            columns: &["instrument_id", "effective_date"],
            predicates: &["ORIGIN = 'MANUAL'"],
        },
        ManualIndexSpec {
            name: "uq_manual_exchange_rate",
            table: "exchange_rate",
            columns: &["rate_date", "base_currency_code", "quote_currency_code"],
            predicates: &["ORIGIN = 'MANUAL'"],
        },
    ]
}

fn validate_origin_definition(connection: &Connection, table_name: &str) -> AppResult<()> {
    let table_sql: String = connection.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table_name],
        |row| row.get(0),
    )?;
    let normalized_sql = table_sql.replace(['\n', '\r', '\t'], " ").to_uppercase();

    if normalized_sql.contains("ORIGIN TEXT NOT NULL DEFAULT 'EXCEL_IMPORT'")
        && normalized_sql.contains("ORIGIN IN ('EXCEL_IMPORT', 'MANUAL')")
    {
        Ok(())
    } else {
        Err(AppError::Validation(format!(
            "資料表 {table_name} 的 origin 欄位定義不符合手動寫入 migration"
        )))
    }
}

fn validate_manual_index(connection: &Connection, spec: &ManualIndexSpec) -> AppResult<()> {
    let (table_name, index_sql): (String, String) = connection.query_row(
        "SELECT tbl_name, sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
        [spec.name],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    if table_name != spec.table {
        return Err(AppError::Validation(format!(
            "索引 {} 建立在錯誤的資料表 {table_name}",
            spec.name
        )));
    }

    let normalized_sql = index_sql.replace(['\n', '\r', '\t'], " ").to_uppercase();

    if !normalized_sql.contains("CREATE UNIQUE INDEX") {
        return Err(AppError::Validation(format!(
            "索引 {} 不是唯一索引",
            spec.name
        )));
    }

    for predicate in spec.predicates {
        if !normalized_sql.contains(predicate) {
            return Err(AppError::Validation(format!(
                "索引 {} 缺少條件 {predicate}",
                spec.name
            )));
        }
    }

    let actual_columns = index_columns(connection, spec.name)?;
    let expected_columns: Vec<String> = spec
        .columns
        .iter()
        .map(|column| column.to_string())
        .collect();
    if actual_columns != expected_columns {
        Err(AppError::Validation(format!(
            "索引 {} 的欄位定義不符合手動寫入 migration",
            spec.name
        )))
    } else {
        Ok(())
    }
}

fn index_columns(connection: &Connection, index_name: &str) -> AppResult<Vec<String>> {
    let mut statement = connection.prepare(&format!("PRAGMA index_info({index_name})"))?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(2)?))
    })?;
    let mut columns = rows.collect::<Result<Vec<_>, _>>()?;
    columns.sort_by_key(|(seqno, _)| *seqno);

    Ok(columns.into_iter().map(|(_, name)| name).collect())
}

fn ensure_origin_column(connection: &Connection, table_name: &str) -> AppResult<()> {
    if column_exists(connection, table_name, "origin")? {
        return Ok(());
    }

    connection.execute(
        &format!(
            "ALTER TABLE {table_name} ADD COLUMN origin TEXT NOT NULL DEFAULT 'EXCEL_IMPORT' CHECK (origin IN ('EXCEL_IMPORT', 'MANUAL'))"
        ),
        [],
    )?;

    Ok(())
}

fn column_exists(connection: &Connection, table_name: &str, column_name: &str) -> AppResult<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table_name})"))?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let exists = columns
        .filter_map(Result::ok)
        .any(|name| name == column_name);
    Ok(exists)
}

pub fn current_version(connection: &Connection) -> AppResult<i64> {
    connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use rusqlite::{params, Connection, ErrorCode};

    use super::{
        column_exists, current_version, migrate, table_exists, validate_fee_rates, AppError,
    };

    #[test]
    fn migrates_old_schema_to_latest_version() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);

        migrate(&mut connection).expect("migration succeeds");

        assert_eq!(current_version(&connection).expect("version"), 10);
        assert!(table_exists(&connection, "ui_preference").expect("preference table"));
        assert!(column_exists(&connection, "holding_snapshot", "origin").expect("column lookup"));
        assert!(index_exists(&connection, "uq_manual_holding_snapshot"));
        assert!(index_exists(&connection, "uq_manual_instrument_price"));
        assert!(index_exists(&connection, "uq_manual_dividend_assumption"));
        assert!(column_exists(&connection, "exchange_rate", "origin").expect("column lookup"));
        assert!(column_exists(&connection, "exchange_rate", "note").expect("column lookup"));
        assert!(index_exists(&connection, "uq_manual_exchange_rate"));
        assert!(
            !column_exists(&connection, "instrument_price", "account_id").expect("column lookup")
        );
        assert!(
            column_exists(&connection, "dividend_assumption", "account_id").expect("column lookup")
        );
    }

    #[test]
    fn migration_is_idempotent_after_version_is_set() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);

        migrate(&mut connection).expect("first migration succeeds");
        migrate(&mut connection).expect("second migration succeeds");

        assert_eq!(current_version(&connection).expect("version"), 10);
    }

    #[test]
    fn migrates_v5_costs_to_fee_inclusive_decimal_text_once() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v5_schema(&connection);
        connection
            .execute(
                "INSERT INTO holding_snapshot (account_id, instrument_id, snapshot_date, quantity_text, average_cost_text, cost_currency_code, origin) VALUES (1, 1, '2026-07-12', '10', '20', 'NTD', 'MANUAL')",
                [],
            )
            .expect("seed v5 snapshot");

        migrate(&mut connection).expect("migrate v5 database");
        let cost_after_first_migration: String = connection
            .query_row(
                "SELECT average_cost_text FROM holding_snapshot WHERE snapshot_date = '2026-07-12'",
                [],
                |row| row.get(0),
            )
            .expect("read migrated cost");
        assert_eq!(cost_after_first_migration, "20.0285");
        let applied_buy_fee_rate: String = connection
            .query_row(
                "SELECT applied_buy_fee_rate FROM holding_snapshot WHERE snapshot_date = '2026-07-12'",
                [],
                |row| row.get(0),
            )
            .expect("read snapshot buy fee rate");
        assert_eq!(applied_buy_fee_rate, "0.001425");
        assert_eq!(current_version(&connection).expect("version"), 10);

        migrate(&mut connection).expect("repeat migration");
        let cost_after_second_migration: String = connection
            .query_row(
                "SELECT average_cost_text FROM holding_snapshot WHERE snapshot_date = '2026-07-12'",
                [],
                |row| row.get(0),
            )
            .expect("read idempotent cost");
        assert_eq!(cost_after_second_migration, "20.0285");
    }

    #[test]
    fn malformed_v5_cost_rolls_back_without_advancing_version() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v5_schema(&connection);
        connection
            .execute(
                "INSERT INTO holding_snapshot (account_id, instrument_id, snapshot_date, quantity_text, average_cost_text, cost_currency_code, origin) VALUES (1, 1, '2026-07-12', '10', 'bad-cost', 'NTD', 'MANUAL')",
                [],
            )
            .expect("seed malformed v5 snapshot");

        let error = migrate(&mut connection).expect_err("malformed cost is rejected");

        assert!(error.to_string().contains("持股快照"));
        assert_eq!(current_version(&connection).expect("version"), 5);
        let cost: String = connection
            .query_row(
                "SELECT average_cost_text FROM holding_snapshot",
                [],
                |row| row.get(0),
            )
            .expect("read unchanged cost");
        assert_eq!(cost, "bad-cost");
    }

    #[test]
    fn negative_v5_cost_rolls_back_without_advancing_version() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v5_schema(&connection);
        connection
            .execute(
                "INSERT INTO holding_snapshot (account_id, instrument_id, snapshot_date, quantity_text, average_cost_text, cost_currency_code, origin) VALUES (1, 1, '2026-07-12', '10', '-20', 'NTD', 'MANUAL')",
                [],
            )
            .expect("seed negative v5 snapshot");

        let error = migrate(&mut connection).expect_err("negative cost is rejected");

        assert!(error.to_string().contains("不可為負數"));
        assert_eq!(current_version(&connection).expect("version"), 5);
    }

    #[test]
    fn holding_metrics_apply_combined_sale_rates_to_liquidation_value() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v5_schema(&connection);
        migrate(&mut connection).expect("migrate schema");
        connection
            .execute(
                "UPDATE instrument SET sell_fee_rate = '0.001425', sell_transaction_tax_rate = '0.003' WHERE instrument_id = 1",
                [],
            )
            .expect("set sale rates");
        connection
            .execute_batch(
                "INSERT INTO holding_snapshot (account_id, instrument_id, snapshot_date, quantity_text, average_cost_text, cost_currency_code, origin) VALUES (1, 1, '2026-07-12', '10', '100', 'NTD', 'MANUAL'); INSERT INTO instrument_price (instrument_id, price_date, price_text, currency_code, origin) VALUES (1, '2026-07-12', '120', 'NTD', 'MANUAL');",
            )
            .expect("seed metrics");

        let (market_value, liquidation_value, profit, return_rate): (f64, f64, f64, f64) = connection
            .query_row(
                "SELECT market_value, liquidation_value, unrealized_profit, unrealized_return_rate FROM v_holding_metrics",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("read fee-aware metrics");
        let expected_liquidation = 1200.0 * (1.0 - 0.001425 - 0.003);
        assert_eq!(market_value, 1200.0);
        assert!((liquidation_value - expected_liquidation).abs() < 1e-9);
        assert!((profit - (expected_liquidation - 1000.0)).abs() < 1e-9);
        assert!((return_rate - ((expected_liquidation - 1000.0) / 1000.0)).abs() < 1e-9);
    }

    #[test]
    fn rejects_invalid_fee_rate_combinations() {
        let error = validate_fee_rates(
            "0.001425".parse().expect("valid rate"),
            "0.6".parse().expect("valid rate"),
            "0.4".parse().expect("valid rate"),
        )
        .expect_err("combined sale rates must be below one");
        assert!(error.to_string().contains("合計必須小於 1"));

        let error = validate_fee_rates(
            "1".parse().expect("valid decimal"),
            "0".parse().expect("valid rate"),
            "0".parse().expect("valid rate"),
        )
        .expect_err("buy rate must be below one");
        assert!(error.to_string().contains("買入手續費率"));
    }

    #[test]
    fn database_rejects_malformed_fee_rate_text() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("migrate schema");

        let error = connection
            .execute(
                "UPDATE instrument SET buy_fee_rate = 'invalid' WHERE instrument_id = 1",
                [],
            )
            .expect_err("malformed fee rate is rejected");

        assert!(error.to_string().contains("fee rates"));
    }

    #[test]
    fn migration_recovers_when_preference_table_exists_before_version_is_set() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("initial migration succeeds");
        connection
            .pragma_update(None, "user_version", 4_i64)
            .expect("rewind version marker");

        migrate(&mut connection).expect("recovery migration succeeds");

        assert_eq!(current_version(&connection).expect("version"), 10);
        assert!(table_exists(&connection, "ui_preference").expect("preference table exists"));
    }

    #[test]
    fn migration_rejects_invalid_preference_table_before_updating_version() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("initial migration succeeds");
        connection
            .execute_batch(
                "DROP TABLE ui_preference; CREATE TABLE ui_preference (preference_key TEXT);",
            )
            .expect("create invalid preference table");
        connection
            .pragma_update(None, "user_version", 4_i64)
            .expect("rewind version marker");

        let error = migrate(&mut connection).expect_err("invalid preference table is rejected");

        assert!(error.to_string().contains("ui_preference"));
        assert_eq!(current_version(&connection).expect("version"), 4);
    }

    #[test]
    fn migration_rejects_invalid_preference_table_at_version_five() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("initial migration succeeds");
        connection
            .execute_batch(
                "DROP TABLE ui_preference; CREATE TABLE ui_preference (preference_key TEXT NOT NULL, scope TEXT NOT NULL, value_text TEXT NOT NULL, PRIMARY KEY (preference_key, scope));",
            )
            .expect("create invalid preference table");

        let error = migrate(&mut connection).expect_err("invalid v5 preference table is rejected");

        assert!(error.to_string().contains("ui_preference"));
        assert_eq!(current_version(&connection).expect("version"), 10);
    }

    #[test]
    fn migration_exposes_dividend_receipt_amount_view() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);

        migrate(&mut connection).expect("migration succeeds");
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
                ) VALUES (1, 1, 1, '2026-07-09', NULL, '0', '0', '1000', 'NTD', 'Manual', 'MANUAL')
                "#,
                [],
            )
            .expect("seed receipt");

        let (receipt_id, account_id, instrument_id, origin, net_amount): (i64, i64, i64, String, f64) =
            connection
                .query_row(
                    "SELECT receipt_id, account_id, instrument_id, origin, net_amount FROM v_dividend_receipt_amount",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )
                .expect("query view");

        assert_eq!(receipt_id, 1);
        assert_eq!(account_id, 1);
        assert_eq!(instrument_id, 1);
        assert_eq!(origin, "MANUAL");
        assert_eq!(net_amount, 1000.0);
    }

    #[test]
    fn migration_deduplicates_account_specific_price_rows_on_new_key() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        connection
            .execute(
                r#"
                INSERT INTO instrument_price (
                    account_id,
                    instrument_id,
                    price_date,
                    price_text,
                    currency_code,
                    source,
                    source_cell
                ) VALUES
                    (2, 1, '2026-07-08', '100', 'NTD', 'A', 'B2'),
                    (1, 1, '2026-07-08', '101', 'NTD', 'A', 'C2')
                "#,
                [],
            )
            .expect("seed duplicate legacy prices");

        migrate(&mut connection).expect("migration succeeds");

        let (count, price_text): (i64, String) = connection
            .query_row(
                "SELECT COUNT(*), price_text FROM instrument_price WHERE instrument_id = 1 AND price_date = '2026-07-08'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("count migrated rows");

        assert_eq!(count, 1);
        assert_eq!(price_text, "101");
    }

    #[test]
    fn migration_recovers_when_origin_column_already_exists_but_version_is_zero() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        connection
            .execute(
                "ALTER TABLE holding_snapshot ADD COLUMN origin TEXT NOT NULL DEFAULT 'EXCEL_IMPORT' CHECK (origin IN ('EXCEL_IMPORT', 'MANUAL'))",
                [],
            )
            .expect("simulate partial migration");

        migrate(&mut connection).expect("migration succeeds");

        assert_eq!(current_version(&connection).expect("version"), 10);
        assert!(
            column_exists(&connection, "account_asset_snapshot", "origin").expect("column lookup")
        );
        assert!(column_exists(&connection, "holding_snapshot", "origin").expect("column lookup"));
    }

    #[test]
    fn prevents_duplicate_manual_holding_snapshot_for_same_date() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("migration succeeds");

        insert_manual_holding(&connection, "2026-07-07").expect("first insert succeeds");
        let error = insert_manual_holding(&connection, "2026-07-07")
            .expect_err("duplicate insert should fail");

        assert_eq!(
            error.sqlite_error_code(),
            Some(ErrorCode::ConstraintViolation)
        );
    }

    #[test]
    fn allows_manual_holding_snapshots_on_different_dates() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("migration succeeds");

        insert_manual_holding(&connection, "2026-07-07").expect("first insert succeeds");
        insert_manual_holding(&connection, "2026-07-08").expect("second insert succeeds");
    }

    #[test]
    fn prevents_duplicate_global_manual_price_for_same_date() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("migration succeeds");

        insert_manual_global_price(&connection, "2026-07-07").expect("first insert succeeds");
        let error = insert_manual_global_price(&connection, "2026-07-07")
            .expect_err("duplicate global price should fail");

        assert_eq!(
            error.sqlite_error_code(),
            Some(ErrorCode::ConstraintViolation)
        );
    }

    #[test]
    fn prevents_duplicate_manual_dividend_assumption_for_same_date() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("migration succeeds");

        insert_manual_dividend_assumption(&connection, 1, "2026-07-07")
            .expect("first insert succeeds");
        let error = insert_manual_dividend_assumption(&connection, 1, "2026-07-07")
            .expect_err("duplicate assumption should fail");

        assert_eq!(
            error.sqlite_error_code(),
            Some(ErrorCode::ConstraintViolation)
        );
    }

    #[test]
    fn allows_manual_dividend_assumptions_for_different_accounts_on_same_date() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("migration succeeds");

        insert_manual_dividend_assumption(&connection, 1, "2026-07-07")
            .expect("first insert succeeds");
        insert_manual_dividend_assumption(&connection, 2, "2026-07-07")
            .expect("second account insert succeeds");
    }

    #[test]
    fn preserves_account_scoped_dividend_assumptions() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        connection
            .execute(
                r#"
                INSERT INTO instrument_price (
                    account_id, instrument_id, price_date, price_text, currency_code, source, source_cell
                ) VALUES (1, 1, '2026-07-07', '10', 'NTD', 'seed', 'A1')
                "#,
                [],
            )
            .expect("first price");
        connection
            .execute(
                r#"
                INSERT INTO instrument_price (
                    account_id, instrument_id, price_date, price_text, currency_code, source, source_cell
                ) VALUES (2, 1, '2026-07-07', '11', 'NTD', 'seed', 'B1')
                "#,
                [],
            )
            .expect("second price");
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    account_id, instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, source_sheet, source_row
                ) VALUES (1, 1, '2026-07-07', '1.5', 'NTD', 'sheet', 1)
                "#,
                [],
            )
            .expect("first assumption");
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    account_id, instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, source_sheet, source_row
                ) VALUES (2, 1, '2026-07-07', '1.8', 'NTD', 'sheet', 2)
                "#,
                [],
            )
            .expect("second assumption");

        migrate(&mut connection).expect("migration succeeds");

        let assumption_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE instrument_id = 1 AND effective_date = '2026-07-07'",
                [],
                |row| row.get(0),
            )
            .expect("count assumption rows");
        let account_one_value: String = connection
            .query_row(
                "SELECT estimated_annual_dividend_per_unit_text FROM dividend_assumption WHERE account_id = 1 AND instrument_id = 1 AND effective_date = '2026-07-07'",
                [],
                |row| row.get(0),
            )
            .expect("account one value");
        let account_two_value: String = connection
            .query_row(
                "SELECT estimated_annual_dividend_per_unit_text FROM dividend_assumption WHERE account_id = 2 AND instrument_id = 1 AND effective_date = '2026-07-07'",
                [],
                |row| row.get(0),
            )
            .expect("account two value");

        assert_eq!(assumption_count, 2);
        assert_eq!(account_one_value, "1.5");
        assert_eq!(account_two_value, "1.8");
    }

    #[test]
    fn preserves_manual_rows_over_later_import_rows_on_same_account() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_stage1_schema(&connection);
        connection
            .execute(
                r#"
                INSERT INTO instrument_price (
                    account_id, instrument_id, price_date, price_text, currency_code, origin
                ) VALUES (1, 1, '2026-07-08', '100', 'NTD', 'MANUAL')
                "#,
                [],
            )
            .expect("manual price row");
        connection
            .execute(
                r#"
                INSERT INTO instrument_price (
                    account_id, instrument_id, price_date, price_text, currency_code, origin
                ) VALUES (2, 1, '2026-07-08', '999', 'NTD', 'EXCEL_IMPORT')
                "#,
                [],
            )
            .expect("later import price row");
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    account_id, instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, origin
                ) VALUES (1, 1, '2026-07-08', '1.2', 'NTD', 'MANUAL')
                "#,
                [],
            )
            .expect("manual assumption row");
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    account_id, instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, origin
                ) VALUES (2, 1, '2026-07-08', '9.9', 'NTD', 'EXCEL_IMPORT')
                "#,
                [],
            )
            .expect("later import assumption row");
        connection
            .pragma_update(None, "user_version", 1_i64)
            .expect("set stage1 version");

        migrate(&mut connection).expect("migration succeeds");

        let (price_text, price_origin): (String, String) = connection
            .query_row(
                "SELECT price_text, origin FROM instrument_price WHERE instrument_id = 1 AND price_date = '2026-07-08'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("migrated price row");
        let (dividend_text, dividend_origin): (String, String) = connection
            .query_row(
                "SELECT estimated_annual_dividend_per_unit_text, origin FROM dividend_assumption WHERE account_id = 1 AND instrument_id = 1 AND effective_date = '2026-07-08'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("migrated assumption row");

        assert_eq!(price_text, "100");
        assert_eq!(price_origin, "MANUAL");
        assert_eq!(dividend_text, "1.2");
        assert_eq!(dividend_origin, "MANUAL");
    }

    #[test]
    fn prefers_manual_exchange_rate_over_import_rate_on_same_date() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("migration succeeds");

        connection
            .execute(
                r#"
                INSERT INTO exchange_rate (
                    rate_date,
                    base_currency_code,
                    quote_currency_code,
                    rate_text,
                    source_sheet,
                    source_cell,
                    origin
                ) VALUES ('2026-07-09', 'USD', 'NTD', '31.1', 'sheet', 'A1', 'EXCEL_IMPORT')
                "#,
                [],
            )
            .expect("import rate row");
        connection
            .execute(
                r#"
                INSERT INTO exchange_rate (
                    rate_date,
                    base_currency_code,
                    quote_currency_code,
                    rate_text,
                    note,
                    origin
                ) VALUES ('2026-07-09', 'USD', 'NTD', '31.25', 'manual', 'MANUAL')
                "#,
                [],
            )
            .expect("manual rate row");

        let (rate_text, origin): (String, String) = connection
            .query_row(
                r#"
                SELECT rate_text, origin
                FROM exchange_rate
                WHERE base_currency_code = 'USD'
                  AND quote_currency_code = 'NTD'
                  AND rate_date <= '2026-07-09'
                ORDER BY rate_date DESC,
                         CASE origin WHEN 'MANUAL' THEN 0 ELSE 1 END,
                         exchange_rate_id DESC
                LIMIT 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("manual-first exchange rate lookup");

        assert_eq!(rate_text, "31.25");
        assert_eq!(origin, "MANUAL");
    }

    #[test]
    fn rejects_future_database_version() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        connection
            .pragma_update(None, "user_version", 999_i64)
            .expect("set future version");

        let error = migrate(&mut connection).expect_err("future version should fail");

        assert!(matches!(error, AppError::Validation(_)));
    }

    #[test]
    fn migrates_version_two_database_without_archive_via_fallback() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v2_product_level_schema_without_archive(&connection);

        migrate(&mut connection).expect("fallback migration succeeds");

        assert_eq!(current_version(&connection).expect("version"), 10);
        assert!(
            table_exists(&connection, "dividend_assumption_account_archive")
                .expect("archive exists")
        );
        assert!(
            column_exists(&connection, "dividend_assumption", "account_id")
                .expect("account scope exists")
        );
    }

    #[test]
    fn migrates_version_three_database_without_archive_via_fallback() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v3_product_level_schema_without_archive(&connection);

        migrate(&mut connection).expect("fallback migration succeeds");

        assert_eq!(current_version(&connection).expect("version"), 10);
        assert!(
            table_exists(&connection, "dividend_assumption_account_archive")
                .expect("archive exists")
        );
        assert!(
            column_exists(&connection, "dividend_assumption", "account_id")
                .expect("account scope exists")
        );
    }

    #[test]
    fn fallback_duplicates_product_level_assumption_to_matching_accounts() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v3_product_level_schema_without_archive(&connection);
        connection
            .execute(
                r#"
                INSERT INTO holding_snapshot (
                    account_id, instrument_id, snapshot_date, quantity_text, average_cost_text, cost_currency_code, origin
                ) VALUES (1, 1, '2026-07-10', '10', '20', 'NTD', 'EXCEL_IMPORT')
                "#,
                [],
            )
            .expect("first account holding");
        connection
            .execute(
                r#"
                INSERT INTO holding_snapshot (
                    account_id, instrument_id, snapshot_date, quantity_text, average_cost_text, cost_currency_code, origin
                ) VALUES (2, 1, '2026-07-10', '10', '20', 'NTD', 'EXCEL_IMPORT')
                "#,
                [],
            )
            .expect("second account holding");
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, origin
                ) VALUES (1, '2026-07-10', '1.25', 'NTD', 'EXCEL_IMPORT')
                "#,
                [],
            )
            .expect("product-level assumption");

        migrate(&mut connection).expect("fallback migration succeeds");

        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE instrument_id = 1 AND effective_date = '2026-07-10'",
                [],
                |row| row.get(0),
            )
            .expect("count duplicated rows");
        assert_eq!(count, 2);
    }

    #[test]
    fn fallback_preserves_unmatched_assumptions_only_in_archive() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v3_product_level_schema_without_archive(&connection);
        connection
            .execute(
                r#"
                INSERT INTO instrument (
                    instrument_id, symbol, name, instrument_type, asset_class, region_type, trading_currency_code
                ) VALUES (2, 'BBB', 'Beta', 'ETF', 'EQUITY', 'DOMESTIC', 'NTD')
                "#,
                [],
            )
            .expect("second instrument");
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, origin
                ) VALUES (2, '2026-07-11', '3.5', 'NTD', 'EXCEL_IMPORT')
                "#,
                [],
            )
            .expect("orphaned assumption");

        migrate(&mut connection).expect("fallback migration succeeds");

        let archive_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption_account_archive WHERE instrument_id = 2 AND effective_date = '2026-07-11'",
                [],
                |row| row.get(0),
            )
            .expect("archive count");
        let active_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE instrument_id = 2 AND effective_date = '2026-07-11'",
                [],
                |row| row.get(0),
            )
            .expect("active count");

        assert_eq!(archive_count, 1);
        assert_eq!(active_count, 0);
    }

    #[test]
    fn account_scoped_v3_schema_repairs_to_v4() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("initial full migration succeeds");
        connection
            .pragma_update(None, "user_version", 3_i64)
            .expect("rewind version marker");

        migrate(&mut connection).expect("repair path succeeds");

        assert_eq!(current_version(&connection).expect("version"), 10);
        assert!(index_exists(&connection, "uq_manual_dividend_assumption"));
    }

    #[test]
    fn archive_path_deduplicates_existing_manual_assumptions() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v3_product_level_schema_with_archive(&connection);

        migrate(&mut connection).expect("archive path migration succeeds");

        let manual_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE account_id = 1 AND instrument_id = 1 AND effective_date = '2026-07-12' AND origin = 'MANUAL'",
                [],
                |row| row.get(0),
            )
            .expect("manual row count");
        assert_eq!(manual_count, 1);
        assert_eq!(current_version(&connection).expect("version"), 10);
    }

    #[test]
    fn archive_path_preserves_rows_created_after_v2() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v3_product_level_schema_with_archive(&connection);
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, origin
                ) VALUES (1, '2026-07-13', '2.2', 'NTD', 'EXCEL_IMPORT')
                "#,
                [],
            )
            .expect("post-v2 current row");

        migrate(&mut connection).expect("archive path migration succeeds");

        let migrated_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE account_id = 1 AND instrument_id = 1 AND effective_date = '2026-07-13'",
                [],
                |row| row.get(0),
            )
            .expect("migrated current row count");
        assert_eq!(migrated_count, 1);
    }

    #[test]
    fn v4_failure_does_not_bump_user_version() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_v3_product_level_schema_without_archive(&connection);
        connection
            .execute("DROP TABLE holding_snapshot", [])
            .expect("remove holding table to force failure");

        let error = migrate(&mut connection).expect_err("v4 migration should fail");

        assert!(error.to_string().contains("holding_snapshot"));
        assert_eq!(current_version(&connection).expect("version"), 3);
    }

    #[test]
    fn batch_transaction_rolls_back_on_failure() {
        let mut connection = Connection::open_in_memory().expect("open test db");
        create_old_schema(&connection);
        migrate(&mut connection).expect("migration succeeds");

        let transaction = connection.transaction().expect("transaction starts");
        insert_manual_holding_in_transaction(&transaction, "2026-07-07")
            .expect("first insert succeeds");
        let duplicate = insert_manual_holding_in_transaction(&transaction, "2026-07-07");
        assert!(duplicate.is_err());
        transaction.rollback().expect("rollback succeeds");

        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM holding_snapshot", [], |row| {
                row.get(0)
            })
            .expect("count rows");
        assert_eq!(count, 0);
    }

    fn create_old_schema(connection: &Connection) {
        connection
            .execute_batch(
                r#"
                CREATE TABLE account (
                    account_id INTEGER PRIMARY KEY,
                    display_name TEXT,
                    institution_id INTEGER,
                    account_type TEXT
                );

                CREATE TABLE person (
                    person_id INTEGER PRIMARY KEY,
                    display_name TEXT
                );

                CREATE TABLE account_owner (
                    account_id INTEGER NOT NULL,
                    person_id INTEGER NOT NULL
                );

                CREATE TABLE instrument (
                    instrument_id INTEGER PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    instrument_type TEXT,
                    asset_class TEXT,
                    region_type TEXT,
                    trading_currency_code TEXT
                );

                CREATE TABLE institution (
                    institution_id INTEGER PRIMARY KEY,
                    name TEXT
                );

                CREATE TABLE currency (
                    currency_code TEXT PRIMARY KEY
                );

                INSERT INTO institution (institution_id, name) VALUES (1, 'Demo Bank');
                INSERT INTO account (account_id, display_name, institution_id, account_type) VALUES (1, 'Account 1', 1, 'BROKERAGE');
                INSERT INTO account (account_id, display_name, institution_id, account_type) VALUES (2, 'Account 2', 1, 'BROKERAGE');
                INSERT INTO person (person_id, display_name) VALUES (1, 'Alex');
                INSERT INTO instrument (instrument_id, symbol, name, instrument_type, asset_class, region_type, trading_currency_code) VALUES (1, 'AAA', 'Alpha', 'ETF', 'EQUITY', 'DOMESTIC', 'NTD');
                INSERT INTO account_owner (account_id, person_id) VALUES (1, 1);
                INSERT INTO account_owner (account_id, person_id) VALUES (2, 1);
                INSERT INTO currency (currency_code) VALUES ('NTD');
                INSERT INTO currency (currency_code) VALUES ('USD');

                CREATE TABLE account_asset_snapshot (
                    snapshot_id INTEGER PRIMARY KEY,
                    account_id INTEGER NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    currency_code TEXT NOT NULL,
                    invested_amount_text TEXT,
                    quantity_text TEXT,
                    unit_value_text TEXT,
                    current_value_override_text TEXT,
                    nature_code TEXT,
                    note TEXT,
                    source_sheet TEXT,
                    source_row INTEGER,
                    UNIQUE (account_id, snapshot_date, asset_type, currency_code, source_row)
                );

                CREATE TABLE holding_snapshot (
                    holding_snapshot_id INTEGER PRIMARY KEY,
                    account_id INTEGER NOT NULL,
                    instrument_id INTEGER NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    quantity_text TEXT,
                    average_cost_text TEXT,
                    cost_currency_code TEXT NOT NULL,
                    note TEXT,
                    source_sheet TEXT,
                    source_row INTEGER,
                    UNIQUE (account_id, instrument_id, snapshot_date, source_row)
                );

                CREATE TABLE instrument_price (
                    price_id INTEGER PRIMARY KEY,
                    account_id INTEGER,
                    instrument_id INTEGER NOT NULL,
                    price_date TEXT NOT NULL,
                    price_text TEXT NOT NULL,
                    currency_code TEXT NOT NULL,
                    source TEXT,
                    source_cell TEXT,
                    UNIQUE (account_id, instrument_id, price_date, source_cell)
                );

                CREATE TABLE dividend_assumption (
                    assumption_id INTEGER PRIMARY KEY,
                    account_id INTEGER NOT NULL,
                    instrument_id INTEGER NOT NULL,
                    effective_date TEXT NOT NULL,
                    payments_per_year INTEGER CHECK (payments_per_year >= 0),
                    latest_dividend_per_unit_text TEXT,
                    estimated_annual_dividend_per_unit_text TEXT,
                    currency_code TEXT NOT NULL,
                    note TEXT,
                    source_sheet TEXT,
                    source_row INTEGER,
                    UNIQUE (account_id, instrument_id, effective_date, source_row)
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
                    CHECK (gross_amount_text IS NOT NULL OR net_amount_override_text IS NOT NULL)
                );

                CREATE TABLE exchange_rate (
                    rate_date TEXT NOT NULL,
                    base_currency_code TEXT NOT NULL,
                    quote_currency_code TEXT NOT NULL,
                    rate_text TEXT NOT NULL,
                    source_sheet TEXT,
                    source_cell TEXT,
                    PRIMARY KEY (rate_date, base_currency_code, quote_currency_code)
                );
                "#,
            )
            .expect("create old schema");
    }

    fn create_stage1_schema(connection: &Connection) {
        create_old_schema(connection);
        for table_name in super::ORIGIN_TABLES {
            super::ensure_origin_column(connection, table_name).expect("add origin column");
        }
        connection
            .execute_batch(super::MANUAL_WRITES_SQL)
            .expect("apply stage1 schema");
    }

    fn create_v5_schema(connection: &Connection) {
        create_stage1_schema(connection);
        connection
            .execute_batch(super::PRODUCT_LEVEL_MARKET_DATA_SQL)
            .expect("apply stage2 schema");
        connection
            .execute_batch(super::EXCHANGE_RATE_MANUAL_ORIGIN_SQL)
            .expect("apply stage3 schema");
        super::migrate_v4_dividend_assumption_account_scope(connection)
            .expect("apply stage4 schema");
        connection
            .execute_batch(super::UI_PREFERENCE_SQL)
            .expect("apply stage5 schema");
        connection
            .pragma_update(None, "user_version", 5_i64)
            .expect("set version 5");
    }

    fn create_v2_product_level_schema_without_archive(connection: &Connection) {
        create_stage1_schema(connection);
        connection
            .execute_batch(super::PRODUCT_LEVEL_MARKET_DATA_SQL)
            .expect("apply stage2 schema");
        connection
            .execute("DROP TABLE dividend_assumption_account_archive", [])
            .expect("remove dividend archive");
        connection
            .pragma_update(None, "user_version", 2_i64)
            .expect("set version 2");
    }

    fn create_v3_product_level_schema_without_archive(connection: &Connection) {
        create_v2_product_level_schema_without_archive(connection);
        connection
            .execute_batch(super::EXCHANGE_RATE_MANUAL_ORIGIN_SQL)
            .expect("apply stage3 schema");
        connection
            .pragma_update(None, "user_version", 3_i64)
            .expect("set version 3");
    }

    fn create_v3_product_level_schema_with_archive(connection: &Connection) {
        create_stage1_schema(connection);
        connection
            .execute(
                r#"
                INSERT INTO holding_snapshot (
                    account_id, instrument_id, snapshot_date, quantity_text, average_cost_text, cost_currency_code, origin
                ) VALUES (1, 1, '2026-07-12', '10', '20', 'NTD', 'MANUAL')
                "#,
                [],
            )
            .expect("seed holding");
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    account_id, instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, origin
                ) VALUES (1, 1, '2026-07-12', '1.7', 'NTD', 'MANUAL')
                "#,
                [],
            )
            .expect("seed manual assumption");
        connection
            .execute_batch(super::PRODUCT_LEVEL_MARKET_DATA_SQL)
            .expect("apply stage2 schema");
        connection
            .execute_batch(super::EXCHANGE_RATE_MANUAL_ORIGIN_SQL)
            .expect("apply stage3 schema");
        connection
            .pragma_update(None, "user_version", 3_i64)
            .expect("set version 3");
    }

    fn index_exists(connection: &Connection, index_name: &str) -> bool {
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [index_name],
                |row| row.get::<_, i64>(0),
            )
            .expect("query index")
            == 1
    }

    fn insert_manual_holding(connection: &Connection, snapshot_date: &str) -> rusqlite::Result<()> {
        connection.execute(
            r#"
            INSERT INTO holding_snapshot (
                account_id,
                instrument_id,
                snapshot_date,
                quantity_text,
                average_cost_text,
                cost_currency_code,
                origin
            ) VALUES (?1, ?2, ?3, '10', '20', 'NTD', 'MANUAL')
            "#,
            params![1_i64, 1_i64, snapshot_date],
        )?;
        Ok(())
    }

    fn insert_manual_holding_in_transaction(
        transaction: &rusqlite::Transaction<'_>,
        snapshot_date: &str,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            r#"
            INSERT INTO holding_snapshot (
                account_id,
                instrument_id,
                snapshot_date,
                quantity_text,
                average_cost_text,
                cost_currency_code,
                origin
            ) VALUES (?1, ?2, ?3, '10', '20', 'NTD', 'MANUAL')
            "#,
            params![1_i64, 1_i64, snapshot_date],
        )?;
        Ok(())
    }

    fn insert_manual_global_price(
        connection: &Connection,
        price_date: &str,
    ) -> rusqlite::Result<()> {
        connection.execute(
            r#"
            INSERT INTO instrument_price (
                instrument_id,
                price_date,
                price_text,
                currency_code,
                origin
            ) VALUES (?1, ?2, '100', 'NTD', 'MANUAL')
            "#,
            params![1_i64, price_date],
        )?;
        Ok(())
    }

    fn insert_manual_dividend_assumption(
        connection: &Connection,
        account_id: i64,
        effective_date: &str,
    ) -> rusqlite::Result<()> {
        connection.execute(
            r#"
            INSERT INTO dividend_assumption (
                account_id,
                instrument_id,
                effective_date,
                estimated_annual_dividend_per_unit_text,
                currency_code,
                origin
            ) VALUES (?1, ?2, ?3, '1.5', 'NTD', 'MANUAL')
            "#,
            params![account_id, 1_i64, effective_date],
        )?;
        Ok(())
    }
}
