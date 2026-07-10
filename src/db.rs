#[cfg(not(target_arch = "wasm32"))]
pub mod backup;
#[cfg(not(target_arch = "wasm32"))]
pub mod migration;
#[cfg(not(target_arch = "wasm32"))]
pub mod path;

#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{Connection, OpenFlags, OptionalExtension, Result as SqlResult};

#[cfg(not(target_arch = "wasm32"))]
use crate::db::{
    backup::{backup_before_migration, backup_for_today},
    migration::migrate,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::error::{AppError, AppResult};
use crate::models::{
    AccountAsset, DashboardSummary, DividendReceiptAccountOption, DividendReceiptFormOptions,
    DividendReceiptInstrumentOption, DividendReceiptRow, ExchangeRatePreview, ExchangeRateRow,
    HoldingMetric, LegacyDividendData,
};

#[cfg(not(target_arch = "wasm32"))]
use crate::models::{LegacyDividendMonthlyRow, LegacyDividendSummaryRow, OwnerAssetTotal};

#[cfg(not(target_arch = "wasm32"))]
pub fn load_holding_metrics() -> Result<Vec<HoldingMetric>, String> {
    load_holding_metrics_native().map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_account_assets() -> Result<Vec<AccountAsset>, String> {
    load_account_assets_native().map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_dashboard_summary() -> Result<DashboardSummary, String> {
    load_dashboard_summary_native().map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_legacy_dividends() -> Result<LegacyDividendData, String> {
    load_legacy_dividends_native().map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_dividend_receipts() -> Result<Vec<DividendReceiptRow>, String> {
    load_dividend_receipts_native().map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_dividend_receipt_form_options() -> Result<DividendReceiptFormOptions, String> {
    load_dividend_receipt_form_options_native().map_err(|error| error.to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_holding_metrics() -> Result<Vec<HoldingMetric>, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_account_assets() -> Result<Vec<AccountAsset>, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_dashboard_summary() -> Result<DashboardSummary, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_legacy_dividends() -> Result<LegacyDividendData, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_dividend_receipts() -> Result<Vec<DividendReceiptRow>, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_dividend_receipt_form_options() -> Result<DividendReceiptFormOptions, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(not(target_arch = "wasm32"))]
fn load_dividend_receipts_native() -> SqlResult<Vec<DividendReceiptRow>> {
    let connection = open_database()?;
    let mut statement = connection.prepare(
        r#"
        SELECT
            r.receipt_id,
            r.account_id,
            r.instrument_id,
            COALESCE(r.origin, 'EXCEL_IMPORT') AS origin,
            COALESCE(GROUP_CONCAT(DISTINCT COALESCE(p.display_name, '未指定')), '未指定') AS owner_name,
            COALESCE(a.display_name, '帳戶 #' || r.account_id) AS account_name,
            COALESCE(i.symbol, '-') AS symbol,
            COALESCE(i.name, '未命名商品') AS instrument_name,
            COALESCE(r.received_on, '-') AS received_on,
            r.gross_amount_text,
            r.tax_amount_text,
            r.fee_amount_text,
            r.net_amount,
            COALESCE(r.currency_code, '-') AS currency_code,
            COALESCE(r.note, '') AS note
        FROM v_dividend_receipt_amount r
        LEFT JOIN account a ON a.account_id = r.account_id
        LEFT JOIN account_owner ao ON ao.account_id = a.account_id
        LEFT JOIN person p ON p.person_id = ao.person_id
        LEFT JOIN instrument i ON i.instrument_id = r.instrument_id
        GROUP BY r.receipt_id
        ORDER BY r.received_on DESC, i.name ASC, r.receipt_id DESC
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok(DividendReceiptRow {
            receipt_id: row.get(0)?,
            account_id: row.get(1)?,
            instrument_id: row.get(2)?,
            origin: row.get(3)?,
            owner_name: row.get(4)?,
            account_name: row.get(5)?,
            symbol: row.get(6)?,
            instrument_name: row.get(7)?,
            received_on: row.get(8)?,
            gross_amount: parse_number_text(row.get(9)?),
            tax_amount: parse_number_text(row.get(10)?),
            fee_amount: parse_number_text(row.get(11)?),
            net_amount: row.get(12)?,
            currency_code: row.get(13)?,
            note: row.get(14)?,
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn load_dividend_receipt_form_options_native() -> SqlResult<DividendReceiptFormOptions> {
    let connection = open_database()?;

    let mut account_statement = connection.prepare(
        r#"
        SELECT
            a.account_id,
            COALESCE(GROUP_CONCAT(DISTINCT COALESCE(p.display_name, '未指定')), '未指定') AS owner_name,
            COALESCE(a.display_name, '帳戶 #' || a.account_id) AS account_name
        FROM account a
        LEFT JOIN account_owner ao ON ao.account_id = a.account_id
        LEFT JOIN person p ON p.person_id = ao.person_id
        GROUP BY a.account_id, a.display_name
        ORDER BY owner_name ASC, account_name ASC
        "#,
    )?;
    let account_rows = account_statement.query_map([], |row| {
        Ok(DividendReceiptAccountOption {
            account_id: row.get(0)?,
            owner_name: row.get(1)?,
            account_name: row.get(2)?,
        })
    })?;

    let mut instrument_statement = connection.prepare(
        r#"
        SELECT
            instrument_id,
            COALESCE(symbol, '-') AS symbol,
            COALESCE(name, '未命名商品') AS instrument_name,
            COALESCE(trading_currency_code, 'NTD') AS currency_code
        FROM instrument
        ORDER BY symbol ASC, instrument_name ASC
        "#,
    )?;
    let instrument_rows = instrument_statement.query_map([], |row| {
        Ok(DividendReceiptInstrumentOption {
            instrument_id: row.get(0)?,
            symbol: row.get(1)?,
            instrument_name: row.get(2)?,
            currency_code: row.get(3)?,
        })
    })?;

    let mut currency_statement = connection.prepare(
        r#"
        SELECT currency_code
        FROM currency
        ORDER BY currency_code ASC
        "#,
    )?;
    let currency_rows = currency_statement.query_map([], |row| row.get(0))?;

    Ok(DividendReceiptFormOptions {
        accounts: account_rows.collect::<Result<Vec<_>, _>>()?,
        instruments: instrument_rows.collect::<Result<Vec<_>, _>>()?,
        currency_codes: currency_rows.collect::<Result<Vec<_>, _>>()?,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn load_legacy_dividends_native() -> SqlResult<LegacyDividendData> {
    let connection = open_database()?;

    Ok(LegacyDividendData {
        summaries: load_legacy_dividend_summaries(&connection)?,
        monthly: load_legacy_dividend_monthly(&connection)?,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn load_legacy_dividend_summaries(
    connection: &Connection,
) -> SqlResult<Vec<LegacyDividendSummaryRow>> {
    let mut statement = connection.prepare(
        r#"
        SELECT
            COALESCE(p.display_name, '未指定') AS owner_name,
            COALESCE(i.symbol, '-') AS symbol,
            COALESCE(i.name, '未命名商品') AS instrument_name,
            s.period_label,
            s.amount_text,
            s.source_cell
        FROM dividend_legacy_summary s
        LEFT JOIN person p ON p.person_id = s.person_id
        LEFT JOIN instrument i ON i.instrument_id = s.instrument_id
        ORDER BY p.display_name ASC, i.name ASC,
            CASE s.period_label
                WHEN 'YEAR_2023' THEN 1
                WHEN 'YEAR_2024' THEN 2
                WHEN 'THROUGH_PREVIOUS_YEAR' THEN 3
                WHEN 'TOTAL_CUMULATIVE' THEN 4
                WHEN 'CURRENT_YEAR_TO_DATE' THEN 5
                ELSE 99
            END,
            s.source_cell ASC
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok(LegacyDividendSummaryRow {
            owner_name: row.get(0)?,
            symbol: row.get(1)?,
            instrument_name: row.get(2)?,
            period_label: row.get(3)?,
            amount: parse_number_text(row.get(4)?),
            source_cell: row.get(5)?,
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn load_legacy_dividend_monthly(
    connection: &Connection,
) -> SqlResult<Vec<LegacyDividendMonthlyRow>> {
    let mut statement = connection.prepare(
        r#"
        SELECT
            COALESCE(p.display_name, '未指定') AS owner_name,
            COALESCE(i.symbol, '-') AS symbol,
            COALESCE(i.name, '未命名商品') AS instrument_name,
            m.series_type,
            m.month_num,
            m.amount_text,
            m.source_cell
        FROM dividend_legacy_monthly m
        LEFT JOIN person p ON p.person_id = m.person_id
        LEFT JOIN instrument i ON i.instrument_id = m.instrument_id
        ORDER BY p.display_name ASC, i.name ASC,
            CASE m.series_type
                WHEN 'ACTUAL_CURRENT_YEAR' THEN 1
                WHEN 'FORECAST_AVERAGE' THEN 2
                ELSE 99
            END,
            m.month_num ASC,
            m.source_cell ASC
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok(LegacyDividendMonthlyRow {
            owner_name: row.get(0)?,
            symbol: row.get(1)?,
            instrument_name: row.get(2)?,
            series_type: row.get(3)?,
            month_num: row.get(4)?,
            amount: parse_number_text(row.get(5)?),
            source_cell: row.get(6)?,
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn load_dashboard_summary_native() -> SqlResult<DashboardSummary> {
    let connection = open_database()?;
    let (total_assets, account_assets, investment_assets) = connection.query_row(
        r#"
        SELECT
            SUM(value_ntd),
            SUM(CASE WHEN source_type = 'ACCOUNT_ASSET' THEN value_ntd END),
            SUM(CASE WHEN source_type = 'HOLDING' THEN value_ntd END)
        FROM v_asset_total
        "#,
        [],
        |row| {
            Ok((
                row.get::<_, Option<f64>>(0)?,
                row.get::<_, Option<f64>>(1)?,
                row.get::<_, Option<f64>>(2)?,
            ))
        },
    )?;

    let owner_totals = load_owner_totals(&connection)?;

    let (account_asset_count, account_asset_missing_value_count, latest_account_asset_date) =
        connection.query_row(
            r#"
        SELECT
            COUNT(*),
            COUNT(CASE WHEN current_value_ntd IS NULL THEN 1 END),
            MAX(snapshot_date)
        FROM v_account_asset_value
        "#,
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )?;

    let (
        holding_count,
        holding_missing_market_value_count,
        holding_missing_dividend_count,
        estimated_annual_dividend,
        latest_holding_date,
    ) = connection.query_row(
        r#"
        SELECT
            COUNT(*),
            SUM(market_value),
            COUNT(CASE WHEN market_value IS NULL THEN 1 END),
            COUNT(CASE WHEN estimated_annual_dividend IS NULL THEN 1 END),
            SUM(estimated_annual_dividend),
            MAX(snapshot_date)
        FROM (
            SELECT
                holding_snapshot_id,
                MAX(market_value) AS market_value,
                MAX(estimated_annual_dividend) AS estimated_annual_dividend,
                MAX(snapshot_date) AS snapshot_date
            FROM v_holding_metrics
            GROUP BY holding_snapshot_id
        )
        "#,
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<f64>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        },
    )?;

    let estimated_monthly_dividend = estimated_annual_dividend.map(|value| value / 12.0);

    Ok(DashboardSummary {
        total_assets,
        account_assets,
        investment_assets,
        account_asset_count,
        holding_count,
        account_asset_missing_value_count,
        holding_missing_market_value_count,
        holding_missing_dividend_count,
        estimated_annual_dividend,
        estimated_monthly_dividend,
        latest_account_asset_date,
        latest_holding_date,
        owner_totals,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn load_owner_totals(connection: &Connection) -> SqlResult<Vec<OwnerAssetTotal>> {
    let mut statement = connection.prepare(
        r#"
        SELECT COALESCE(owner_name, '未指定') AS owner_name, SUM(value_ntd)
        FROM v_asset_total
        GROUP BY owner_name
        ORDER BY SUM(value_ntd) DESC
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok(OwnerAssetTotal {
            owner_name: row.get(0)?,
            value_ntd: row.get(1)?,
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn load_account_assets_native() -> SqlResult<Vec<AccountAsset>> {
    let connection = open_database()?;
    let mut statement = connection.prepare(
        r#"
        SELECT
            v.snapshot_id,
            v.account_id,
            s.origin,
            COALESCE(p.display_name, '未指定') AS owner_name,
            COALESCE(i.name, '未指定機構') AS institution_name,
            COALESCE(a.display_name, '帳戶 #' || v.account_id) AS account_name,
            COALESCE(a.account_type, 'UNKNOWN') AS account_type,
            COALESCE(v.asset_type, 'UNKNOWN') AS asset_type,
            COALESCE(v.currency_code, '-') AS currency_code,
            v.quantity_text,
            v.current_value_override_text,
            v.invested_amount_text,
            v.current_value_ntd,
            COALESCE(v.snapshot_date, '-') AS snapshot_date,
            COALESCE(s.note, '') AS note
        FROM v_account_asset_value v
        JOIN account_asset_snapshot s ON s.snapshot_id = v.snapshot_id
        LEFT JOIN account a ON a.account_id = v.account_id
        LEFT JOIN institution i ON i.institution_id = a.institution_id
        LEFT JOIN account_owner ao ON ao.account_id = a.account_id
        LEFT JOIN person p ON p.person_id = ao.person_id
        ORDER BY v.current_value_ntd DESC, i.name ASC, a.display_name ASC, v.asset_type ASC
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        let quantity_text: Option<String> = row.get(9)?;
        let invested_amount_text: Option<String> = row.get(11)?;

        Ok(AccountAsset {
            snapshot_id: row.get(0)?,
            account_id: row.get(1)?,
            origin: row.get(2)?,
            owner_name: row.get(3)?,
            institution_name: row.get(4)?,
            account_name: row.get(5)?,
            account_type: row.get(6)?,
            asset_type: row.get(7)?,
            currency_code: row.get(8)?,
            quantity_text: quantity_text.clone(),
            current_value_override_text: row.get(10)?,
            invested_amount_text: invested_amount_text.clone(),
            current_value_ntd: row.get(12)?,
            snapshot_date: row.get(13)?,
            note: row.get(14)?,
            quantity: parse_number_text(quantity_text),
            invested_amount: parse_number_text(invested_amount_text),
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_applicable_exchange_rate(
    currency_code: &str,
    snapshot_date: &str,
) -> Result<Option<ExchangeRatePreview>, String> {
    if currency_code == "NTD" {
        return Ok(Some(ExchangeRatePreview {
            rate_text: "1".to_string(),
            rate_date: snapshot_date.to_string(),
        }));
    }

    let connection = open_database().map_err(|e| e.to_string())?;

    let result = connection
        .query_row(
            r#"
            SELECT rate_text, rate_date
            FROM exchange_rate
            WHERE base_currency_code = ?1
              AND quote_currency_code = 'NTD'
              AND rate_date <= ?2
            ORDER BY rate_date DESC,
                     CASE origin WHEN 'MANUAL' THEN 0 ELSE 1 END,
                     exchange_rate_id DESC
            LIMIT 1
            "#,
            rusqlite::params![currency_code, snapshot_date],
            |row| {
                Ok(ExchangeRatePreview {
                    rate_text: row.get(0)?,
                    rate_date: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(|e| e.to_string())?;

    Ok(result)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_recent_exchange_rates(limit: usize) -> Result<Vec<ExchangeRateRow>, String> {
    let connection = open_database().map_err(|e| e.to_string())?;
    let mut statement = connection
        .prepare(
            r#"
            SELECT
                exchange_rate_id,
                rate_date,
                base_currency_code,
                quote_currency_code,
                rate_text,
                origin,
                COALESCE(note, '') AS note
            FROM exchange_rate
            ORDER BY rate_date DESC,
                     base_currency_code ASC,
                     quote_currency_code ASC,
                     CASE origin WHEN 'MANUAL' THEN 0 ELSE 1 END,
                     exchange_rate_id DESC
            LIMIT ?1
            "#,
        )
        .map_err(|e| e.to_string())?;

    let rows = statement
        .query_map(rusqlite::params![limit as i64], |row| {
            Ok(ExchangeRateRow {
                exchange_rate_id: row.get(0)?,
                rate_date: row.get(1)?,
                base_currency_code: row.get(2)?,
                quote_currency_code: row.get(3)?,
                rate_text: row.get(4)?,
                origin: row.get(5)?,
                note: row.get(6)?,
            })
        })
        .map_err(|e| e.to_string())?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
fn parse_number_text(value: Option<String>) -> Option<f64> {
    let normalized = value?.trim().replace(',', "");
    if normalized.is_empty() {
        return None;
    }

    normalized.parse::<f64>().ok()
}

#[cfg(not(target_arch = "wasm32"))]
fn load_holding_metrics_native() -> SqlResult<Vec<HoldingMetric>> {
    let connection = open_database()?;
    let mut statement = connection.prepare(
        r#"
        SELECT
            h.holding_snapshot_id,
            h.account_id,
            h.instrument_id,
            COALESCE(h.owner_name, '未指定') AS owner_name,
            COALESCE(a.display_name, '帳戶 #' || h.account_id) AS account_name,
            COALESCE(h.symbol, '-') AS symbol,
            COALESCE(h.instrument_name, '未命名商品') AS instrument_name,
            COALESCE(h.instrument_type, 'UNKNOWN') AS instrument_type,
            COALESCE(h.asset_class, 'UNKNOWN') AS asset_class,
            COALESCE(h.region_type, 'UNKNOWN') AS region_type,
            COALESCE(h.trading_currency_code, 'NTD') AS trading_currency_code,
            COALESCE(s.cost_currency_code, h.trading_currency_code, 'NTD') AS cost_currency_code,
            COALESCE(h.snapshot_date, '-') AS snapshot_date,
            COALESCE(s.note, '') AS note,
            h.quantity,
            h.average_cost,
            h.market_price_date,
            h.market_price_currency_code,
            h.market_price,
            h.total_cost,
            h.market_value,
            h.unrealized_profit,
            h.unrealized_return_rate,
            h.dividend_effective_date,
            h.dividend_currency_code,
            h.estimated_annual_dividend_per_unit,
            h.payments_per_year,
            h.latest_dividend_per_unit,
            h.estimated_annual_dividend,
            h.estimated_yield_on_cost
        FROM v_holding_metrics h
        LEFT JOIN holding_snapshot s ON s.holding_snapshot_id = h.holding_snapshot_id
        LEFT JOIN account a ON a.account_id = h.account_id
        ORDER BY h.market_value DESC, h.instrument_name ASC
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok(HoldingMetric {
            holding_snapshot_id: row.get(0)?,
            account_id: row.get(1)?,
            instrument_id: row.get(2)?,
            owner_name: row.get(3)?,
            account_name: row.get(4)?,
            symbol: row.get(5)?,
            instrument_name: row.get(6)?,
            instrument_type: row.get(7)?,
            asset_class: row.get(8)?,
            region_type: row.get(9)?,
            trading_currency_code: row.get(10)?,
            cost_currency_code: row.get(11)?,
            snapshot_date: row.get(12)?,
            note: row.get(13)?,
            quantity: row.get(14)?,
            average_cost: row.get(15)?,
            market_price_date: row.get(16)?,
            market_price_currency_code: row.get(17)?,
            market_price: row.get(18)?,
            total_cost: row.get(19)?,
            market_value: row.get(20)?,
            unrealized_profit: row.get(21)?,
            unrealized_return_rate: row.get(22)?,
            dividend_effective_date: row.get(23)?,
            dividend_currency_code: row.get(24)?,
            estimated_annual_dividend_per_unit: row.get(25)?,
            payments_per_year: row.get(26)?,
            latest_dividend_per_unit: row.get(27)?,
            estimated_annual_dividend: row.get(28)?,
            estimated_yield_on_cost: row.get(29)?,
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_migrated_database() -> AppResult<std::path::PathBuf> {
    let path = path::ensure_runtime_database()?;
    let mut connection = Connection::open(&path)?;
    let current_version = migration::current_version(&connection)?;
    if current_version < migration::latest_version() {
        backup_before_migration(&path, current_version)?;
    }
    migrate(&mut connection)?;

    Ok(path)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn open_database() -> SqlResult<Connection> {
    let path = ensure_migrated_database().map_err(app_error_to_sql_error)?;
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
pub fn open_writable_database() -> AppResult<Connection> {
    let path = ensure_migrated_database()?;
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(connection)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn open_manual_write_database() -> AppResult<Connection> {
    let path = ensure_migrated_database()?;
    backup_for_today(&path)?;
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(connection)
}

#[cfg(not(target_arch = "wasm32"))]
fn app_error_to_sql_error(error: AppError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}
