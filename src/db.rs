#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{Connection, Result as SqlResult};

use crate::models::{AccountAsset, DashboardSummary, HoldingMetric, LegacyDividendData};

#[cfg(not(target_arch = "wasm32"))]
use crate::models::{LegacyDividendMonthlyRow, LegacyDividendSummaryRow};

#[cfg(not(target_arch = "wasm32"))]
const DATABASE_PATH: &str = "assets/data.sqlite";

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

#[cfg(not(target_arch = "wasm32"))]
fn load_legacy_dividends_native() -> SqlResult<LegacyDividendData> {
    let connection = Connection::open(database_path())?;

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
    let connection = Connection::open(database_path())?;
    let (
        account_asset_count,
        account_assets,
        account_asset_missing_value_count,
        latest_account_asset_date,
    ) = connection.query_row(
        r#"
        SELECT
            COUNT(*),
            SUM(current_value_ntd),
            COUNT(CASE WHEN current_value_ntd IS NULL THEN 1 END),
            MAX(snapshot_date)
        FROM v_account_asset_value
        "#,
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<f64>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        },
    )?;

    let (
        holding_count,
        investment_assets,
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
                row.get::<_, Option<f64>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<f64>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        },
    )?;

    let total_assets = add_optional(account_assets, investment_assets);
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
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn add_optional(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn load_account_assets_native() -> SqlResult<Vec<AccountAsset>> {
    let connection = Connection::open(database_path())?;
    let mut statement = connection.prepare(
        r#"
        SELECT
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
            COALESCE(v.snapshot_date, '-') AS snapshot_date
        FROM v_account_asset_value v
        LEFT JOIN account a ON a.account_id = v.account_id
        LEFT JOIN institution i ON i.institution_id = a.institution_id
        LEFT JOIN account_owner ao ON ao.account_id = a.account_id
        LEFT JOIN person p ON p.person_id = ao.person_id
        ORDER BY v.current_value_ntd DESC, i.name ASC, a.display_name ASC, v.asset_type ASC
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok(AccountAsset {
            owner_name: row.get(0)?,
            institution_name: row.get(1)?,
            account_name: row.get(2)?,
            account_type: row.get(3)?,
            asset_type: row.get(4)?,
            currency_code: row.get(5)?,
            original_amount: first_number([
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
            ]),
            current_value_ntd: row.get(9)?,
            snapshot_date: row.get(10)?,
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn first_number<const N: usize>(values: [Option<String>; N]) -> Option<f64> {
    values.into_iter().find_map(parse_number_text)
}

#[cfg(not(target_arch = "wasm32"))]
fn parse_number_text(value: Option<String>) -> Option<f64> {
    let normalized = value?.trim().replace(',', "");
    if normalized.is_empty() {
        return None;
    }

    normalized.parse::<f64>().ok()
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn parses_first_valid_number_text() {
        assert_eq!(
            first_number([Some("".to_string()), Some("1,234.5".to_string())]),
            Some(1234.5)
        );
    }

    #[test]
    fn ignores_invalid_number_text() {
        assert_eq!(
            first_number([Some("not a number".to_string()), Some("42".to_string())]),
            Some(42.0)
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn load_holding_metrics_native() -> SqlResult<Vec<HoldingMetric>> {
    let connection = Connection::open(database_path())?;
    let mut statement = connection.prepare(
        r#"
        SELECT
            COALESCE(h.owner_name, '未指定') AS owner_name,
            COALESCE(a.display_name, '帳戶 #' || h.account_id) AS account_name,
            COALESCE(h.symbol, '-') AS symbol,
            COALESCE(h.instrument_name, '未命名商品') AS instrument_name,
            COALESCE(h.instrument_type, 'UNKNOWN') AS instrument_type,
            COALESCE(h.asset_class, 'UNKNOWN') AS asset_class,
            COALESCE(h.region_type, 'UNKNOWN') AS region_type,
            COALESCE(h.snapshot_date, '-') AS snapshot_date,
            h.quantity,
            h.average_cost,
            h.market_price,
            h.total_cost,
            h.market_value,
            h.unrealized_profit,
            h.unrealized_return_rate,
            h.estimated_annual_dividend,
            h.estimated_yield_on_cost
        FROM v_holding_metrics h
        LEFT JOIN account a ON a.account_id = h.account_id
        ORDER BY h.market_value DESC, h.instrument_name ASC
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok(HoldingMetric {
            owner_name: row.get(0)?,
            account_name: row.get(1)?,
            symbol: row.get(2)?,
            instrument_name: row.get(3)?,
            instrument_type: row.get(4)?,
            asset_class: row.get(5)?,
            region_type: row.get(6)?,
            snapshot_date: row.get(7)?,
            quantity: row.get(8)?,
            average_cost: row.get(9)?,
            market_price: row.get(10)?,
            total_cost: row.get(11)?,
            market_value: row.get(12)?,
            unrealized_profit: row.get(13)?,
            unrealized_return_rate: row.get(14)?,
            estimated_annual_dividend: row.get(15)?,
            estimated_yield_on_cost: row.get(16)?,
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn database_path() -> PathBuf {
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            let exe_relative_path = exe_dir.join(DATABASE_PATH);
            if exe_relative_path.exists() {
                return exe_relative_path;
            }
        }
    }

    let manifest_relative_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DATABASE_PATH);
    if manifest_relative_path.exists() {
        return manifest_relative_path;
    }

    PathBuf::from(DATABASE_PATH)
}
