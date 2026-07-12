use rust_decimal::Decimal;

#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{params, Connection, OptionalExtension, Transaction};

#[cfg(not(target_arch = "wasm32"))]
use crate::db::migration::validate_fee_rates;
#[cfg(not(target_arch = "wasm32"))]
use crate::db::open_manual_write_database;
use crate::decimal::{normalize_decimal_text, parse_decimal_field};
use crate::error::{AppError, AppResult};

#[derive(Clone, Debug, PartialEq)]
pub struct CurrentHoldingUpdateInput {
    pub account_id: i64,
    pub instrument_id: i64,
    pub as_of_date: String,
    pub quantity_text: String,
    pub average_cost_text: String,
    pub market_price_text: String,
    pub currency_code: String,
    pub payments_per_year_text: String,
    pub latest_dividend_per_unit_text: String,
    pub estimated_annual_dividend_per_unit_text: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CurrentHoldingStateInput {
    pub account_id: i64,
    pub instrument_id: i64,
    pub as_of_date: String,
    pub quantity_text: String,
    pub average_cost_text: String,
    pub currency_code: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DividendAssumptionInput {
    pub account_id: i64,
    pub instrument_id: i64,
    pub effective_date: String,
    pub payments_per_year_text: String,
    pub latest_dividend_per_unit_text: String,
    pub estimated_annual_dividend_per_unit_text: String,
    pub currency_code: String,
}

#[derive(Clone, Debug, PartialEq)]
struct ValidatedHoldingUpdate {
    account_id: i64,
    instrument_id: i64,
    as_of_date: String,
    quantity_text: String,
    average_cost_text: String,
    market_price_text: String,
    currency_code: String,
    payments_per_year: Option<i64>,
    latest_dividend_per_unit_text: Option<String>,
    estimated_annual_dividend_per_unit_text: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct ValidatedHoldingState {
    account_id: i64,
    instrument_id: i64,
    as_of_date: String,
    quantity_text: String,
    average_cost_text: String,
    currency_code: String,
    note: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct ValidatedDividendAssumption {
    account_id: i64,
    instrument_id: i64,
    effective_date: String,
    payments_per_year: Option<i64>,
    latest_dividend_per_unit_text: Option<String>,
    estimated_annual_dividend_per_unit_text: Option<String>,
    currency_code: String,
}

#[allow(dead_code)]
pub fn validate_current_holding_update(
    input: &CurrentHoldingUpdateInput,
) -> AppResult<CurrentHoldingUpdateInput> {
    let validated = validate_current_holding_update_inner(input)?;

    Ok(CurrentHoldingUpdateInput {
        account_id: validated.account_id,
        instrument_id: validated.instrument_id,
        as_of_date: validated.as_of_date,
        quantity_text: validated.quantity_text,
        average_cost_text: validated.average_cost_text,
        market_price_text: validated.market_price_text,
        currency_code: validated.currency_code,
        payments_per_year_text: validated
            .payments_per_year
            .map(|value| value.to_string())
            .unwrap_or_default(),
        latest_dividend_per_unit_text: validated.latest_dividend_per_unit_text.unwrap_or_default(),
        estimated_annual_dividend_per_unit_text: validated
            .estimated_annual_dividend_per_unit_text
            .unwrap_or_default(),
    })
}

#[allow(dead_code)]
pub fn validate_current_holding_state_input(
    input: &CurrentHoldingStateInput,
) -> AppResult<CurrentHoldingStateInput> {
    let validated = validate_current_holding_state_inner(input)?;

    Ok(CurrentHoldingStateInput {
        account_id: validated.account_id,
        instrument_id: validated.instrument_id,
        as_of_date: validated.as_of_date,
        quantity_text: validated.quantity_text,
        average_cost_text: validated.average_cost_text,
        currency_code: validated.currency_code,
        note: validated.note.unwrap_or_default(),
    })
}

#[allow(dead_code)]
pub fn validate_dividend_assumption_input(
    input: &DividendAssumptionInput,
) -> AppResult<DividendAssumptionInput> {
    let validated = validate_dividend_assumption_inner(input)?;

    Ok(DividendAssumptionInput {
        account_id: validated.account_id,
        instrument_id: validated.instrument_id,
        effective_date: validated.effective_date,
        payments_per_year_text: validated
            .payments_per_year
            .map(|value| value.to_string())
            .unwrap_or_default(),
        latest_dividend_per_unit_text: validated.latest_dividend_per_unit_text.unwrap_or_default(),
        estimated_annual_dividend_per_unit_text: validated
            .estimated_annual_dividend_per_unit_text
            .unwrap_or_default(),
        currency_code: validated.currency_code,
    })
}

#[allow(dead_code)]
#[cfg(not(target_arch = "wasm32"))]
pub fn save_current_holding_update(input: CurrentHoldingUpdateInput) -> AppResult<()> {
    let mut connection = open_manual_write_database()?;
    save_current_holding_update_with_connection(&mut connection, input)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn save_current_holding_state(input: CurrentHoldingStateInput) -> AppResult<()> {
    let mut connection = open_manual_write_database()?;
    save_current_holding_state_with_connection(&mut connection, input)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn save_dividend_assumption(input: DividendAssumptionInput) -> AppResult<()> {
    let mut connection = open_manual_write_database()?;
    save_dividend_assumption_with_connection(&mut connection, input)
}

#[cfg(target_arch = "wasm32")]
pub fn save_current_holding_state(_input: CurrentHoldingStateInput) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 持股更新".to_string(),
    ))
}

#[cfg(target_arch = "wasm32")]
pub fn save_dividend_assumption(_input: DividendAssumptionInput) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 持股更新".to_string(),
    ))
}

#[cfg(target_arch = "wasm32")]
pub fn save_current_holding_update(_input: CurrentHoldingUpdateInput) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 持股更新".to_string(),
    ))
}

fn validate_current_holding_update_inner(
    input: &CurrentHoldingUpdateInput,
) -> AppResult<ValidatedHoldingUpdate> {
    let as_of_date = input.as_of_date.trim().to_string();
    if as_of_date.is_empty() {
        return Err(AppError::Validation("請輸入更新日期".to_string()));
    }
    if !is_iso_date(&as_of_date) {
        return Err(AppError::Validation(
            "更新日期格式必須為 YYYY-MM-DD".to_string(),
        ));
    }

    let currency_code = input.currency_code.trim().to_string();
    if currency_code.is_empty() {
        return Err(AppError::Validation("請輸入幣別".to_string()));
    }

    let quantity = parse_decimal_field("quantity", &input.quantity_text)?;
    if quantity.is_sign_negative() {
        return Err(AppError::Validation("持有數量不可為負數".to_string()));
    }

    let average_cost = parse_decimal_field("average_cost", &input.average_cost_text)?;
    if average_cost.is_sign_negative() {
        return Err(AppError::Validation("平均成本不可為負數".to_string()));
    }

    let market_price = parse_decimal_field("market_price", &input.market_price_text)?;
    if market_price <= Decimal::ZERO {
        return Err(AppError::Validation("目前市價必須大於 0".to_string()));
    }

    let payments_per_year = parse_optional_payments_per_year(&input.payments_per_year_text)?;
    let latest_dividend_per_unit_text = parse_optional_decimal_text(
        "latest_dividend_per_unit",
        &input.latest_dividend_per_unit_text,
    )?;
    let estimated_annual_dividend_per_unit_text = parse_optional_decimal_text(
        "estimated_annual_dividend_per_unit",
        &input.estimated_annual_dividend_per_unit_text,
    )?;

    Ok(ValidatedHoldingUpdate {
        account_id: input.account_id,
        instrument_id: input.instrument_id,
        as_of_date,
        quantity_text: normalize_decimal_text(quantity),
        average_cost_text: normalize_decimal_text(average_cost),
        market_price_text: normalize_decimal_text(market_price),
        currency_code,
        payments_per_year,
        latest_dividend_per_unit_text,
        estimated_annual_dividend_per_unit_text,
    })
}

fn validate_current_holding_state_inner(
    input: &CurrentHoldingStateInput,
) -> AppResult<ValidatedHoldingState> {
    let as_of_date = input.as_of_date.trim().to_string();
    if as_of_date.is_empty() {
        return Err(AppError::Validation("請輸入更新日期".to_string()));
    }
    if !is_iso_date(&as_of_date) {
        return Err(AppError::Validation(
            "更新日期格式必須為 YYYY-MM-DD".to_string(),
        ));
    }

    let currency_code = input.currency_code.trim().to_string();
    if currency_code.is_empty() {
        return Err(AppError::Validation("請輸入幣別".to_string()));
    }

    let quantity = parse_decimal_field("quantity", &input.quantity_text)?;
    if quantity.is_sign_negative() {
        return Err(AppError::Validation("持有數量不可為負數".to_string()));
    }

    let average_cost = parse_decimal_field("average_cost", &input.average_cost_text)?;
    if average_cost.is_sign_negative() {
        return Err(AppError::Validation("平均成本不可為負數".to_string()));
    }

    Ok(ValidatedHoldingState {
        account_id: input.account_id,
        instrument_id: input.instrument_id,
        as_of_date,
        quantity_text: normalize_decimal_text(quantity),
        average_cost_text: normalize_decimal_text(average_cost),
        currency_code,
        note: normalize_optional_text(&input.note),
    })
}

fn validate_dividend_assumption_inner(
    input: &DividendAssumptionInput,
) -> AppResult<ValidatedDividendAssumption> {
    let effective_date = input.effective_date.trim().to_string();
    if effective_date.is_empty() {
        return Err(AppError::Validation("請輸入生效日期".to_string()));
    }
    if !is_iso_date(&effective_date) {
        return Err(AppError::Validation(
            "生效日期格式必須為 YYYY-MM-DD".to_string(),
        ));
    }

    let currency_code = input.currency_code.trim().to_string();
    if currency_code.is_empty() {
        return Err(AppError::Validation("請輸入幣別".to_string()));
    }

    let payments_per_year = parse_optional_payments_per_year(&input.payments_per_year_text)?;
    let latest_dividend_per_unit_text = parse_optional_decimal_text(
        "latest_dividend_per_unit",
        &input.latest_dividend_per_unit_text,
    )?;
    let estimated_annual_dividend_per_unit_text = parse_optional_decimal_text(
        "estimated_annual_dividend_per_unit",
        &input.estimated_annual_dividend_per_unit_text,
    )?;

    Ok(ValidatedDividendAssumption {
        account_id: input.account_id,
        instrument_id: input.instrument_id,
        effective_date,
        payments_per_year,
        latest_dividend_per_unit_text,
        estimated_annual_dividend_per_unit_text,
        currency_code,
    })
}

fn parse_optional_payments_per_year(input: &str) -> AppResult<Option<i64>> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let value = trimmed
        .parse::<i64>()
        .map_err(|_| AppError::Validation("配息頻率必須是整數".to_string()))?;
    if value <= 0 {
        return Err(AppError::Validation("配息頻率必須大於 0".to_string()));
    }

    Ok(Some(value))
}

fn parse_optional_decimal_text(field: &'static str, input: &str) -> AppResult<Option<String>> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let value = parse_decimal_field(field, trimmed)?;
    if value.is_sign_negative() {
        return Err(AppError::Validation("配息數值不可為負數".to_string()));
    }

    Ok(Some(normalize_decimal_text(value)))
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
fn save_current_holding_update_with_connection(
    connection: &mut Connection,
    input: CurrentHoldingUpdateInput,
) -> AppResult<()> {
    let validated = validate_current_holding_update_inner(&input)?;
    let transaction = connection.transaction()?;

    ensure_current_or_newer_holding_date(
        &transaction,
        validated.account_id,
        validated.instrument_id,
        &validated.as_of_date,
    )?;

    let buy_fee_rate = instrument_buy_fee_rate(&transaction, validated.instrument_id)?;
    upsert_holding_snapshot(&transaction, &validated, buy_fee_rate)?;
    upsert_instrument_price(&transaction, &validated)?;
    upsert_dividend_assumption(
        &transaction,
        validated.account_id,
        validated.instrument_id,
        &validated.as_of_date,
        &validated.currency_code,
        validated.payments_per_year,
        validated.latest_dividend_per_unit_text,
        validated.estimated_annual_dividend_per_unit_text,
    )?;

    transaction.commit()?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn save_dividend_assumption_with_connection(
    connection: &mut Connection,
    input: DividendAssumptionInput,
) -> AppResult<()> {
    let validated = validate_dividend_assumption_inner(&input)?;
    let transaction = connection.transaction()?;

    upsert_dividend_assumption(
        &transaction,
        validated.account_id,
        validated.instrument_id,
        &validated.effective_date,
        &validated.currency_code,
        validated.payments_per_year,
        validated.latest_dividend_per_unit_text,
        validated.estimated_annual_dividend_per_unit_text,
    )?;

    transaction.commit()?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn save_current_holding_state_with_connection(
    connection: &mut Connection,
    input: CurrentHoldingStateInput,
) -> AppResult<()> {
    let validated = validate_current_holding_state_inner(&input)?;
    let transaction = connection.transaction()?;

    ensure_current_or_newer_holding_date(
        &transaction,
        validated.account_id,
        validated.instrument_id,
        &validated.as_of_date,
    )?;

    let buy_fee_rate = instrument_buy_fee_rate(&transaction, validated.instrument_id)?;
    upsert_holding_state_snapshot(&transaction, &validated, buy_fee_rate)?;

    transaction.commit()?;
    Ok(())
}

#[allow(dead_code)]
#[cfg(not(target_arch = "wasm32"))]
fn upsert_holding_snapshot(
    transaction: &Transaction<'_>,
    update: &ValidatedHoldingUpdate,
    buy_fee_rate: Decimal,
) -> AppResult<()> {
    let existing_id = transaction
        .query_row(
            r#"
            SELECT holding_snapshot_id, average_cost_text, applied_buy_fee_rate
            FROM holding_snapshot
            WHERE account_id = ?1
              AND instrument_id = ?2
              AND snapshot_date = ?3
              AND origin = 'MANUAL'
            ORDER BY holding_snapshot_id DESC
            LIMIT 1
            "#,
            params![update.account_id, update.instrument_id, update.as_of_date],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;

    let (fee_inclusive_average_cost, applied_buy_fee_rate) = fee_inclusive_cost_for_save(
        &update.average_cost_text,
        buy_fee_rate,
        existing_id
            .as_ref()
            .map(|(_, average_cost_text, applied_buy_fee_rate)| {
                (average_cost_text.as_str(), applied_buy_fee_rate.as_str())
            }),
    )?;
    if let Some((holding_snapshot_id, _, _)) = existing_id {
        transaction.execute(
            r#"
            UPDATE holding_snapshot
            SET quantity_text = ?1,
                average_cost_text = ?2,
                applied_buy_fee_rate = ?3,
                cost_currency_code = ?4,
                source_sheet = NULL,
                source_row = NULL,
                origin = 'MANUAL'
            WHERE holding_snapshot_id = ?5
            "#,
            params![
                update.quantity_text,
                fee_inclusive_average_cost,
                applied_buy_fee_rate,
                update.currency_code,
                holding_snapshot_id,
            ],
        )?;
    } else {
        transaction.execute(
            r#"
            INSERT INTO holding_snapshot (
                account_id,
                instrument_id,
                snapshot_date,
                quantity_text,
                average_cost_text,
                applied_buy_fee_rate,
                cost_currency_code,
                origin
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'MANUAL')
            "#,
            params![
                update.account_id,
                update.instrument_id,
                update.as_of_date,
                update.quantity_text,
                fee_inclusive_average_cost,
                applied_buy_fee_rate,
                update.currency_code,
            ],
        )?;
    }

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn upsert_holding_state_snapshot(
    transaction: &Transaction<'_>,
    update: &ValidatedHoldingState,
    buy_fee_rate: Decimal,
) -> AppResult<()> {
    let existing_id = transaction
        .query_row(
            r#"
            SELECT holding_snapshot_id, average_cost_text, applied_buy_fee_rate
            FROM holding_snapshot
            WHERE account_id = ?1
              AND instrument_id = ?2
              AND snapshot_date = ?3
              AND origin = 'MANUAL'
            ORDER BY holding_snapshot_id DESC
            LIMIT 1
            "#,
            params![update.account_id, update.instrument_id, update.as_of_date],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;

    let (fee_inclusive_average_cost, applied_buy_fee_rate) = fee_inclusive_cost_for_save(
        &update.average_cost_text,
        buy_fee_rate,
        existing_id
            .as_ref()
            .map(|(_, average_cost_text, applied_buy_fee_rate)| {
                (average_cost_text.as_str(), applied_buy_fee_rate.as_str())
            }),
    )?;
    if let Some((holding_snapshot_id, _, _)) = existing_id {
        transaction.execute(
            r#"
            UPDATE holding_snapshot
            SET quantity_text = ?1,
                average_cost_text = ?2,
                applied_buy_fee_rate = ?3,
                cost_currency_code = ?4,
                note = ?5,
                source_sheet = NULL,
                source_row = NULL,
                origin = 'MANUAL'
            WHERE holding_snapshot_id = ?6
            "#,
            params![
                update.quantity_text,
                fee_inclusive_average_cost,
                applied_buy_fee_rate,
                update.currency_code,
                update.note,
                holding_snapshot_id,
            ],
        )?;
    } else {
        transaction.execute(
            r#"
            INSERT INTO holding_snapshot (
                account_id,
                instrument_id,
                snapshot_date,
                quantity_text,
                average_cost_text,
                applied_buy_fee_rate,
                cost_currency_code,
                note,
                origin
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'MANUAL')
            "#,
            params![
                update.account_id,
                update.instrument_id,
                update.as_of_date,
                update.quantity_text,
                fee_inclusive_average_cost,
                applied_buy_fee_rate,
                update.currency_code,
                update.note,
            ],
        )?;
    }

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn fee_inclusive_cost_for_save(
    fee_exclusive_average_cost_text: &str,
    current_buy_fee_rate: Decimal,
    existing: Option<(&str, &str)>,
) -> AppResult<(String, String)> {
    let fee_exclusive_average_cost =
        parse_decimal_field("average_cost", fee_exclusive_average_cost_text)?;
    if let Some((stored_cost_text, stored_fee_rate_text)) = existing {
        let stored_cost = parse_decimal_field("average_cost", stored_cost_text)?;
        let stored_fee_rate = parse_decimal_field("applied_buy_fee_rate", stored_fee_rate_text)?;
        if fee_exclusive_average_cost == stored_cost / (Decimal::ONE + stored_fee_rate) {
            return Ok((
                stored_cost_text.to_string(),
                stored_fee_rate_text.to_string(),
            ));
        }
    }
    Ok((
        normalize_decimal_text(fee_exclusive_average_cost * (Decimal::ONE + current_buy_fee_rate)),
        normalize_decimal_text(current_buy_fee_rate),
    ))
}

#[cfg(not(target_arch = "wasm32"))]
fn instrument_buy_fee_rate(
    transaction: &Transaction<'_>,
    instrument_id: i64,
) -> AppResult<Decimal> {
    let (buy_fee_rate, sell_fee_rate, sell_transaction_tax_rate): (String, String, String) = transaction
        .query_row(
            "SELECT buy_fee_rate, sell_fee_rate, sell_transaction_tax_rate FROM instrument WHERE instrument_id = ?1",
            [instrument_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?
        .ok_or_else(|| AppError::Validation(format!("找不到商品 {instrument_id} 的費率設定")))?;
    let buy_fee_rate = parse_decimal_field("buy_fee_rate", &buy_fee_rate)?;
    let sell_fee_rate = parse_decimal_field("sell_fee_rate", &sell_fee_rate)?;
    let sell_transaction_tax_rate =
        parse_decimal_field("sell_transaction_tax_rate", &sell_transaction_tax_rate)?;
    validate_fee_rates(buy_fee_rate, sell_fee_rate, sell_transaction_tax_rate)?;
    Ok(buy_fee_rate)
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_current_or_newer_holding_date(
    transaction: &Transaction<'_>,
    account_id: i64,
    instrument_id: i64,
    as_of_date: &str,
) -> AppResult<()> {
    let latest_snapshot_date: Option<String> = transaction
        .query_row(
            r#"
            SELECT MAX(snapshot_date)
            FROM holding_snapshot
            WHERE account_id = ?1
              AND instrument_id = ?2
            "#,
            params![account_id, instrument_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();

    if let Some(latest_snapshot_date) = latest_snapshot_date {
        if as_of_date < latest_snapshot_date.as_str() {
            return Err(AppError::Validation(format!(
                "目前持股狀態不可寫入早於現有最新資料日 {latest_snapshot_date} 的日期"
            )));
        }
    }

    Ok(())
}

#[allow(dead_code)]
#[cfg(not(target_arch = "wasm32"))]
fn upsert_instrument_price(
    transaction: &Transaction<'_>,
    update: &ValidatedHoldingUpdate,
) -> AppResult<()> {
    let existing_id = transaction
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
            params![update.instrument_id, update.as_of_date],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    if let Some(price_id) = existing_id {
        transaction.execute(
            r#"
            UPDATE instrument_price
            SET price_text = ?1,
                currency_code = ?2,
                origin = 'MANUAL'
            WHERE price_id = ?3
            "#,
            params![update.market_price_text, update.currency_code, price_id],
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
                update.instrument_id,
                update.as_of_date,
                update.market_price_text,
                update.currency_code,
            ],
        )?;
    }

    Ok(())
}

#[allow(dead_code)]
#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_arguments)]
fn upsert_dividend_assumption(
    transaction: &Transaction<'_>,
    account_id: i64,
    instrument_id: i64,
    effective_date: &str,
    currency_code: &str,
    payments_per_year: Option<i64>,
    latest_dividend_per_unit_text: Option<String>,
    estimated_annual_dividend_per_unit_text: Option<String>,
) -> AppResult<()> {
    let existing_id = transaction
        .query_row(
            r#"
            SELECT assumption_id
            FROM dividend_assumption
            WHERE account_id = ?1
              AND instrument_id = ?2
              AND effective_date = ?3
              AND origin = 'MANUAL'
            ORDER BY assumption_id DESC
            LIMIT 1
            "#,
            params![account_id, instrument_id, effective_date],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    let should_write = existing_id.is_some()
        || payments_per_year.is_some()
        || latest_dividend_per_unit_text.is_some()
        || estimated_annual_dividend_per_unit_text.is_some();
    if !should_write {
        return Ok(());
    }

    if let Some(assumption_id) = existing_id {
        transaction.execute(
            r#"
            UPDATE dividend_assumption
            SET payments_per_year = ?1,
                latest_dividend_per_unit_text = ?2,
                estimated_annual_dividend_per_unit_text = ?3,
                currency_code = ?4,
                origin = 'MANUAL'
            WHERE assumption_id = ?5
            "#,
            params![
                payments_per_year,
                latest_dividend_per_unit_text,
                estimated_annual_dividend_per_unit_text,
                currency_code,
                assumption_id,
            ],
        )?;
    } else {
        transaction.execute(
            r#"
            INSERT INTO dividend_assumption (
                account_id,
                instrument_id,
                effective_date,
                payments_per_year,
                latest_dividend_per_unit_text,
                estimated_annual_dividend_per_unit_text,
                currency_code,
                origin
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'MANUAL')
            "#,
            params![
                account_id,
                instrument_id,
                effective_date,
                payments_per_year,
                latest_dividend_per_unit_text,
                estimated_annual_dividend_per_unit_text,
                currency_code,
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
    use super::save_current_holding_update_with_connection;
    #[cfg(not(target_arch = "wasm32"))]
    use super::save_dividend_assumption_with_connection;
    #[cfg(not(target_arch = "wasm32"))]
    use super::{fee_inclusive_cost_for_save, save_current_holding_state_with_connection};
    use super::{
        validate_current_holding_state_input, validate_current_holding_update,
        validate_dividend_assumption_input, CurrentHoldingStateInput, CurrentHoldingUpdateInput,
        DividendAssumptionInput,
    };
    #[cfg(not(target_arch = "wasm32"))]
    use crate::db::migration::migrate;
    use crate::error::AppError;

    fn sample_input() -> CurrentHoldingUpdateInput {
        CurrentHoldingUpdateInput {
            account_id: 1,
            instrument_id: 1,
            as_of_date: "2099-01-01".to_string(),
            quantity_text: "100".to_string(),
            average_cost_text: "50".to_string(),
            market_price_text: "55".to_string(),
            currency_code: "NTD".to_string(),
            payments_per_year_text: "4".to_string(),
            latest_dividend_per_unit_text: "0.5".to_string(),
            estimated_annual_dividend_per_unit_text: "2.0".to_string(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn preserves_existing_cost_and_fee_when_average_cost_is_unchanged() {
        let current_buy_fee_rate = "0.002".parse().expect("valid current fee");

        let result = fee_inclusive_cost_for_save(
            "45.5",
            current_buy_fee_rate,
            Some(("45.5648375", "0.001425")),
        )
        .expect("preserve existing cost");

        assert_eq!(result, ("45.5648375".to_string(), "0.001425".to_string()));
    }

    fn sample_state_input() -> CurrentHoldingStateInput {
        CurrentHoldingStateInput {
            account_id: 1,
            instrument_id: 1,
            as_of_date: "2099-01-01".to_string(),
            quantity_text: "100".to_string(),
            average_cost_text: "50".to_string(),
            currency_code: "NTD".to_string(),
            note: "長期持有".to_string(),
        }
    }

    #[test]
    fn normalizes_valid_holding_update_input() {
        let input = CurrentHoldingUpdateInput {
            quantity_text: "00100.5000".to_string(),
            average_cost_text: "050.00".to_string(),
            market_price_text: "055.000".to_string(),
            latest_dividend_per_unit_text: "0.5000".to_string(),
            estimated_annual_dividend_per_unit_text: "2.0000".to_string(),
            ..sample_input()
        };

        let normalized = validate_current_holding_update(&input).expect("valid input");

        assert_eq!(normalized.quantity_text, "100.5");
        assert_eq!(normalized.average_cost_text, "50");
        assert_eq!(normalized.market_price_text, "55");
        assert_eq!(normalized.latest_dividend_per_unit_text, "0.5");
        assert_eq!(normalized.estimated_annual_dividend_per_unit_text, "2");
    }

    #[test]
    fn rejects_zero_market_price() {
        let input = CurrentHoldingUpdateInput {
            market_price_text: "0".to_string(),
            ..sample_input()
        };

        let error = validate_current_holding_update(&input).expect_err("zero price should fail");
        assert!(matches!(error, AppError::Validation(_)));
    }

    #[test]
    fn rejects_invalid_update_date() {
        let input = CurrentHoldingUpdateInput {
            as_of_date: "2099-99-99".to_string(),
            ..sample_input()
        };

        let error = validate_current_holding_update(&input).expect_err("invalid date should fail");
        assert!(matches!(error, AppError::Validation(_)));
    }

    #[test]
    fn normalizes_valid_holding_state_input() {
        let input = CurrentHoldingStateInput {
            quantity_text: "00100.5000".to_string(),
            average_cost_text: "050.00".to_string(),
            note: "  逢低加碼  ".to_string(),
            ..sample_state_input()
        };

        let normalized = validate_current_holding_state_input(&input).expect("valid state input");

        assert_eq!(normalized.quantity_text, "100.5");
        assert_eq!(normalized.average_cost_text, "50");
        assert_eq!(normalized.note, "逢低加碼");
    }

    #[test]
    fn rejects_negative_holding_state_quantity() {
        let input = CurrentHoldingStateInput {
            quantity_text: "-1".to_string(),
            ..sample_state_input()
        };

        let error = validate_current_holding_state_input(&input)
            .expect_err("negative quantity should fail");
        assert!(matches!(error, AppError::Validation(_)));
    }

    #[test]
    fn normalizes_valid_dividend_assumption_input() {
        let input = DividendAssumptionInput {
            account_id: 1,
            instrument_id: 1,
            effective_date: "2099-03-01".to_string(),
            payments_per_year_text: "04".to_string(),
            latest_dividend_per_unit_text: "0.5000".to_string(),
            estimated_annual_dividend_per_unit_text: "2.0000".to_string(),
            currency_code: "NTD".to_string(),
        };

        let normalized = validate_dividend_assumption_input(&input).expect("valid dividend input");

        assert_eq!(normalized.payments_per_year_text, "4");
        assert_eq!(normalized.latest_dividend_per_unit_text, "0.5");
        assert_eq!(normalized.estimated_annual_dividend_per_unit_text, "2");
    }

    #[test]
    fn rejects_invalid_dividend_assumption_date() {
        let input = DividendAssumptionInput {
            effective_date: "2099-13-01".to_string(),
            ..DividendAssumptionInput {
                account_id: 1,
                instrument_id: 1,
                effective_date: "2099-03-01".to_string(),
                payments_per_year_text: "4".to_string(),
                latest_dividend_per_unit_text: "0.5".to_string(),
                estimated_annual_dividend_per_unit_text: "2.0".to_string(),
                currency_code: "NTD".to_string(),
            }
        };

        let error =
            validate_dividend_assumption_input(&input).expect_err("invalid date should fail");
        assert!(matches!(error, AppError::Validation(_)));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn saves_dividend_assumption_updates_in_place_for_same_date() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        migrate(&mut connection).expect("migrate temp db");

        let (account_id, instrument_id, currency_code): (i64, i64, String) = connection
            .query_row(
                r#"
                SELECT account_id, instrument_id, trading_currency_code
                FROM v_holding_metrics
                ORDER BY market_value DESC, holding_snapshot_id DESC
                LIMIT 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("seed holding row");

        let first_input = DividendAssumptionInput {
            account_id,
            instrument_id,
            effective_date: "2099-03-01".to_string(),
            payments_per_year_text: "4".to_string(),
            latest_dividend_per_unit_text: "0.5".to_string(),
            estimated_annual_dividend_per_unit_text: "2.0".to_string(),
            currency_code: currency_code.clone(),
        };
        save_dividend_assumption_with_connection(&mut connection, first_input)
            .expect("first dividend save");

        let second_input = DividendAssumptionInput {
            latest_dividend_per_unit_text: "0.6".to_string(),
            estimated_annual_dividend_per_unit_text: "2.4".to_string(),
            ..DividendAssumptionInput {
                account_id,
                instrument_id,
                effective_date: "2099-03-01".to_string(),
                payments_per_year_text: "4".to_string(),
                latest_dividend_per_unit_text: "0.5".to_string(),
                estimated_annual_dividend_per_unit_text: "2.0".to_string(),
                currency_code,
            }
        };
        save_dividend_assumption_with_connection(&mut connection, second_input)
            .expect("second dividend save");

        let (payments_per_year, latest_text, annual_text): (Option<i64>, String, String) = connection
            .query_row(
                r#"
                SELECT payments_per_year, latest_dividend_per_unit_text, estimated_annual_dividend_per_unit_text
                FROM dividend_assumption
                WHERE account_id = ?1 AND instrument_id = ?2 AND effective_date = '2099-03-01' AND origin = 'MANUAL'
                ORDER BY assumption_id DESC
                LIMIT 1
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("saved dividend row");

        assert_eq!(payments_per_year, Some(4));
        assert_eq!(latest_text, "0.6");
        assert_eq!(annual_text, "2.4");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn preserves_dividend_assumption_history_for_different_effective_dates() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        migrate(&mut connection).expect("migrate temp db");

        let (account_id, instrument_id, currency_code): (i64, i64, String) = connection
            .query_row(
                r#"
                SELECT account_id, instrument_id, trading_currency_code
                FROM v_holding_metrics
                ORDER BY market_value DESC, holding_snapshot_id DESC
                LIMIT 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("seed holding row");

        let before_row_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2",
                params![account_id, instrument_id],
                |row| row.get(0),
            )
            .expect("count dividend rows before save");

        save_dividend_assumption_with_connection(
            &mut connection,
            DividendAssumptionInput {
                account_id,
                instrument_id,
                effective_date: "2099-03-01".to_string(),
                payments_per_year_text: "4".to_string(),
                latest_dividend_per_unit_text: "0.5".to_string(),
                estimated_annual_dividend_per_unit_text: "2.0".to_string(),
                currency_code: currency_code.clone(),
            },
        )
        .expect("first dividend save");
        save_dividend_assumption_with_connection(
            &mut connection,
            DividendAssumptionInput {
                account_id,
                instrument_id,
                effective_date: "2099-03-02".to_string(),
                payments_per_year_text: "2".to_string(),
                latest_dividend_per_unit_text: "0.8".to_string(),
                estimated_annual_dividend_per_unit_text: "1.6".to_string(),
                currency_code,
            },
        )
        .expect("second dividend save");

        let row_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2",
                params![account_id, instrument_id],
                |row| row.get(0),
            )
            .expect("count dividend rows");
        assert_eq!(row_count, before_row_count + 2);

        let (quantity, dividend_effective_date, per_unit, annual_dividend): (f64, String, f64, f64) = connection
            .query_row(
                r#"
                SELECT quantity, dividend_effective_date, estimated_annual_dividend_per_unit, estimated_annual_dividend
                FROM v_holding_metrics
                WHERE account_id = ?1 AND instrument_id = ?2
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("latest dividend view row");

        assert_eq!(dividend_effective_date, "2099-03-02");
        assert_eq!(per_unit, 1.6);
        assert!((annual_dividend - quantity * 1.6).abs() < 1e-9);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn saves_and_refreshes_product_level_holding_data() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        migrate(&mut connection).expect("migrate temp db");

        let (account_id, instrument_id, currency_code): (i64, i64, String) = connection
            .query_row(
                r#"
                SELECT account_id, instrument_id, COALESCE(market_price_currency_code, trading_currency_code)
                FROM v_holding_metrics
                ORDER BY market_value DESC, holding_snapshot_id DESC
                LIMIT 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("seed holding row");

        let input = CurrentHoldingUpdateInput {
            account_id,
            instrument_id,
            currency_code,
            as_of_date: "2099-01-01".to_string(),
            quantity_text: "321".to_string(),
            average_cost_text: "45.5".to_string(),
            market_price_text: "47.25".to_string(),
            payments_per_year_text: "4".to_string(),
            latest_dividend_per_unit_text: "0.6".to_string(),
            estimated_annual_dividend_per_unit_text: "2.4".to_string(),
        };

        save_current_holding_update_with_connection(&mut connection, input.clone())
            .expect("save holding update");
        save_current_holding_update_with_connection(&mut connection, input)
            .expect("same-day update rewrites current rows");

        let (quantity_text, average_cost_text): (String, String) = connection
            .query_row(
                r#"
                SELECT quantity_text, average_cost_text
                FROM holding_snapshot
                WHERE account_id = ?1 AND instrument_id = ?2 AND snapshot_date = '2099-01-01'
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("holding snapshot row");
        assert_eq!(quantity_text, "321");
        assert_eq!(average_cost_text, "45.5648375");

        let price_row_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-01-01'",
                params![instrument_id],
                |row| row.get(0),
            )
            .expect("count price rows");
        assert_eq!(price_row_count, 1);

        let assumption_row_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2 AND effective_date = '2099-01-01'",
                params![account_id, instrument_id],
                |row| row.get(0),
            )
            .expect("count assumption rows");
        assert_eq!(assumption_row_count, 1);

        let (market_price, estimated_dividend): (f64, f64) = connection
            .query_row(
                r#"
                SELECT market_price, estimated_annual_dividend
                FROM v_holding_metrics
                WHERE account_id = ?1 AND instrument_id = ?2
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("updated holding metric");
        assert_eq!(market_price, 47.25);
        assert_eq!(estimated_dividend, 321.0 * 2.4);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn prefers_existing_manual_rows_over_later_import_rows_on_same_date() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        migrate(&mut connection).expect("migrate temp db");

        let (account_id, instrument_id, currency_code): (i64, i64, String) = connection
            .query_row(
                r#"
                SELECT account_id, instrument_id, COALESCE(market_price_currency_code, trading_currency_code)
                FROM v_holding_metrics
                ORDER BY market_value DESC, holding_snapshot_id DESC
                LIMIT 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("seed holding row");

        let input = CurrentHoldingUpdateInput {
            account_id,
            instrument_id,
            currency_code: currency_code.clone(),
            as_of_date: "2099-01-02".to_string(),
            quantity_text: "111".to_string(),
            average_cost_text: "40".to_string(),
            market_price_text: "41".to_string(),
            payments_per_year_text: "2".to_string(),
            latest_dividend_per_unit_text: "0.4".to_string(),
            estimated_annual_dividend_per_unit_text: "0.8".to_string(),
        };

        save_current_holding_update_with_connection(&mut connection, input.clone())
            .expect("first manual save");

        connection
            .execute(
                r#"
                INSERT INTO holding_snapshot (
                    account_id, instrument_id, snapshot_date, quantity_text, average_cost_text, cost_currency_code, origin
                ) VALUES (?1, ?2, ?3, '999', '999', ?4, 'EXCEL_IMPORT')
                "#,
                params![account_id, instrument_id, input.as_of_date, currency_code],
            )
            .expect("later import holding");
        connection
            .execute(
                r#"
                INSERT INTO instrument_price (
                    instrument_id, price_date, price_text, currency_code, origin
                ) VALUES (?1, ?2, '999', ?3, 'EXCEL_IMPORT')
                "#,
                params![instrument_id, input.as_of_date, currency_code],
            )
            .expect("later import price");
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    account_id, instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, origin
                ) VALUES (?1, ?2, ?3, '9.9', ?4, 'EXCEL_IMPORT')
                "#,
                params![account_id, instrument_id, input.as_of_date, currency_code],
            )
            .expect("later import assumption");

        let (view_quantity, view_price): (f64, f64) = connection
            .query_row(
                r#"
                SELECT quantity, market_price
                FROM v_holding_metrics
                WHERE account_id = ?1 AND instrument_id = ?2
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("manual rows still drive holding view");
        assert_eq!(view_quantity, 111.0);
        assert_eq!(view_price, 41.0);

        let second_input = CurrentHoldingUpdateInput {
            quantity_text: "222".to_string(),
            average_cost_text: "42".to_string(),
            market_price_text: "44".to_string(),
            estimated_annual_dividend_per_unit_text: "1.2".to_string(),
            ..input
        };
        save_current_holding_update_with_connection(&mut connection, second_input)
            .expect("second manual save");

        let (quantity_text, price_text, dividend_text): (String, String, String) = connection
            .query_row(
                r#"
                SELECT
                    (SELECT quantity_text FROM holding_snapshot WHERE account_id = ?1 AND instrument_id = ?2 AND snapshot_date = '2099-01-02' AND origin = 'MANUAL' ORDER BY holding_snapshot_id DESC LIMIT 1),
                    (SELECT price_text FROM instrument_price WHERE instrument_id = ?2 AND price_date = '2099-01-02' AND origin = 'MANUAL' ORDER BY price_id DESC LIMIT 1),
                    (SELECT estimated_annual_dividend_per_unit_text FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2 AND effective_date = '2099-01-02' AND origin = 'MANUAL' ORDER BY assumption_id DESC LIMIT 1)
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("manual rows persist");

        assert_eq!(quantity_text, "222");
        assert_eq!(price_text, "44");
        assert_eq!(dividend_text, "1.2");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn first_manual_holding_update_preserves_existing_import_price_and_assumption_rows() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        migrate(&mut connection).expect("migrate temp db");

        let (account_id, instrument_id, currency_code): (i64, i64, String) = connection
            .query_row(
                r#"
                SELECT account_id, instrument_id, COALESCE(market_price_currency_code, trading_currency_code)
                FROM v_holding_metrics
                ORDER BY market_value DESC, holding_snapshot_id DESC
                LIMIT 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("seed holding row");

        connection
            .execute(
                r#"
                INSERT INTO instrument_price (
                    instrument_id, price_date, price_text, currency_code, origin, source_cell
                ) VALUES (?1, '2099-01-03', '88', ?2, 'EXCEL_IMPORT', 'A1')
                "#,
                params![instrument_id, currency_code],
            )
            .expect("seed import price");
        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    account_id, instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, origin, source_row
                ) VALUES (?1, ?2, '2099-01-03', '1.1', ?3, 'EXCEL_IMPORT', 1)
                "#,
                params![account_id, instrument_id, currency_code],
            )
            .expect("seed import assumption");

        save_current_holding_update_with_connection(
            &mut connection,
            CurrentHoldingUpdateInput {
                account_id,
                instrument_id,
                currency_code: currency_code.clone(),
                as_of_date: "2099-01-03".to_string(),
                quantity_text: "10".to_string(),
                average_cost_text: "20".to_string(),
                market_price_text: "30".to_string(),
                payments_per_year_text: "4".to_string(),
                latest_dividend_per_unit_text: "0.5".to_string(),
                estimated_annual_dividend_per_unit_text: "2.0".to_string(),
            },
        )
        .expect("save manual holding update");

        let (import_price_count, manual_price_count): (i64, i64) = connection
            .query_row(
                r#"
                SELECT
                    (SELECT COUNT(*) FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-01-03' AND origin = 'EXCEL_IMPORT'),
                    (SELECT COUNT(*) FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-01-03' AND origin = 'MANUAL')
                "#,
                params![instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("count price rows");
        assert_eq!(import_price_count, 1);
        assert_eq!(manual_price_count, 1);

        let (import_assumption_count, manual_assumption_count): (i64, i64) = connection
            .query_row(
                r#"
                SELECT
                    (SELECT COUNT(*) FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2 AND effective_date = '2099-01-03' AND origin = 'EXCEL_IMPORT'),
                    (SELECT COUNT(*) FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2 AND effective_date = '2099-01-03' AND origin = 'MANUAL')
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("count assumption rows");
        assert_eq!(import_assumption_count, 1);
        assert_eq!(manual_assumption_count, 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn saves_holding_state_without_touching_price_or_dividend_rows() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        migrate(&mut connection).expect("migrate temp db");

        let (account_id, instrument_id, currency_code): (i64, i64, String) = connection
            .query_row(
                r#"
                SELECT account_id, instrument_id, trading_currency_code
                FROM v_holding_metrics
                ORDER BY market_value DESC, holding_snapshot_id DESC
                LIMIT 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("seed holding row");

        let before_price_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-02-01'",
                params![instrument_id],
                |row| row.get(0),
            )
            .expect("count price rows before save");
        let before_dividend_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2 AND effective_date = '2099-02-01'",
                params![account_id, instrument_id],
                |row| row.get(0),
            )
            .expect("count dividend rows before save");

        let input = CurrentHoldingStateInput {
            account_id,
            instrument_id,
            currency_code,
            as_of_date: "2099-02-01".to_string(),
            quantity_text: "432.1".to_string(),
            average_cost_text: "48.2".to_string(),
            note: "分批建立倉位".to_string(),
        };

        save_current_holding_state_with_connection(&mut connection, input.clone())
            .expect("first state save");

        let (quantity_text, average_cost_text, note, origin): (
            String,
            String,
            Option<String>,
            String,
        ) = connection
            .query_row(
                r#"
                    SELECT quantity_text, average_cost_text, note, origin
                    FROM holding_snapshot
                    WHERE account_id = ?1 AND instrument_id = ?2 AND snapshot_date = ?3
                    ORDER BY holding_snapshot_id DESC
                    LIMIT 1
                    "#,
                params![account_id, instrument_id, input.as_of_date],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("saved holding state row");
        assert_eq!(quantity_text, "432.1");
        assert_eq!(average_cost_text, "48.268685");
        assert_eq!(note.as_deref(), Some("分批建立倉位"));
        assert_eq!(origin, "MANUAL");

        let after_price_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM instrument_price WHERE instrument_id = ?1 AND price_date = '2099-02-01'",
                params![instrument_id],
                |row| row.get(0),
            )
            .expect("count price rows after save");
        let after_dividend_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2 AND effective_date = '2099-02-01'",
                params![account_id, instrument_id],
                |row| row.get(0),
            )
            .expect("count dividend rows after save");
        assert_eq!(after_price_count, before_price_count);
        assert_eq!(after_dividend_count, before_dividend_count);

        let update_input = CurrentHoldingStateInput {
            quantity_text: "500".to_string(),
            average_cost_text: "50".to_string(),
            note: String::new(),
            ..input
        };
        save_current_holding_state_with_connection(&mut connection, update_input)
            .expect("same-day state update");

        let (updated_quantity, updated_note, manual_count): (String, Option<String>, i64) = connection
            .query_row(
                r#"
                SELECT
                    (SELECT quantity_text FROM holding_snapshot WHERE account_id = ?1 AND instrument_id = ?2 AND snapshot_date = '2099-02-01' AND origin = 'MANUAL' ORDER BY holding_snapshot_id DESC LIMIT 1),
                    (SELECT note FROM holding_snapshot WHERE account_id = ?1 AND instrument_id = ?2 AND snapshot_date = '2099-02-01' AND origin = 'MANUAL' ORDER BY holding_snapshot_id DESC LIMIT 1),
                    (SELECT COUNT(*) FROM holding_snapshot WHERE account_id = ?1 AND instrument_id = ?2 AND snapshot_date = '2099-02-01' AND origin = 'MANUAL')
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("updated same-day state row");
        assert_eq!(updated_quantity, "500");
        assert_eq!(updated_note, None);
        assert_eq!(manual_count, 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn first_manual_dividend_assumption_preserves_existing_import_row() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        migrate(&mut connection).expect("migrate temp db");

        let (account_id, instrument_id, currency_code): (i64, i64, String) = connection
            .query_row(
                r#"
                SELECT account_id, instrument_id, trading_currency_code
                FROM v_holding_metrics
                ORDER BY market_value DESC, holding_snapshot_id DESC
                LIMIT 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("seed holding row");

        connection
            .execute(
                r#"
                INSERT INTO dividend_assumption (
                    account_id, instrument_id, effective_date, estimated_annual_dividend_per_unit_text, currency_code, origin, source_row
                ) VALUES (?1, ?2, '2099-02-02', '1.1', ?3, 'EXCEL_IMPORT', 1)
                "#,
                params![account_id, instrument_id, currency_code],
            )
            .expect("seed import assumption");

        save_dividend_assumption_with_connection(
            &mut connection,
            DividendAssumptionInput {
                account_id,
                instrument_id,
                effective_date: "2099-02-02".to_string(),
                payments_per_year_text: "4".to_string(),
                latest_dividend_per_unit_text: "0.3".to_string(),
                estimated_annual_dividend_per_unit_text: "1.2".to_string(),
                currency_code: currency_code.clone(),
            },
        )
        .expect("save manual assumption");

        let (import_count, manual_count): (i64, i64) = connection
            .query_row(
                r#"
                SELECT
                    (SELECT COUNT(*) FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2 AND effective_date = '2099-02-02' AND origin = 'EXCEL_IMPORT'),
                    (SELECT COUNT(*) FROM dividend_assumption WHERE account_id = ?1 AND instrument_id = ?2 AND effective_date = '2099-02-02' AND origin = 'MANUAL')
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("count assumption rows");
        assert_eq!(import_count, 1);
        assert_eq!(manual_count, 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn state_save_creates_manual_row_without_modifying_excel_import_row() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        migrate(&mut connection).expect("migrate temp db");

        let (account_id, instrument_id, currency_code): (i64, i64, String) = connection
            .query_row(
                r#"
                SELECT account_id, instrument_id, trading_currency_code
                FROM v_holding_metrics
                ORDER BY market_value DESC, holding_snapshot_id DESC
                LIMIT 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("seed holding row");

        connection
            .execute(
                r#"
                INSERT INTO holding_snapshot (
                    account_id, instrument_id, snapshot_date, quantity_text, average_cost_text, cost_currency_code, note, source_sheet, source_row, origin
                ) VALUES (?1, ?2, '2099-03-01', '999', '88', ?3, 'import note', 'sheet1', 7, 'EXCEL_IMPORT')
                "#,
                params![account_id, instrument_id, currency_code],
            )
            .expect("insert import holding row");

        let input = CurrentHoldingStateInput {
            account_id,
            instrument_id,
            currency_code,
            as_of_date: "2099-03-01".to_string(),
            quantity_text: "123".to_string(),
            average_cost_text: "45".to_string(),
            note: "manual note".to_string(),
        };

        save_current_holding_state_with_connection(&mut connection, input)
            .expect("state save succeeds");

        let (import_origin, import_note, import_source_sheet, import_source_row): (
            String,
            Option<String>,
            Option<String>,
            Option<i64>,
        ) = connection
            .query_row(
                r#"
                SELECT origin, note, source_sheet, source_row
                FROM holding_snapshot
                WHERE account_id = ?1 AND instrument_id = ?2 AND snapshot_date = '2099-03-01' AND origin = 'EXCEL_IMPORT'
                ORDER BY holding_snapshot_id DESC
                LIMIT 1
                "#,
                params![account_id, instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("excel import row preserved");
        assert_eq!(import_origin, "EXCEL_IMPORT");
        assert_eq!(import_note.as_deref(), Some("import note"));
        assert_eq!(import_source_sheet.as_deref(), Some("sheet1"));
        assert_eq!(import_source_row, Some(7));

        let (manual_quantity, manual_origin, manual_note): (String, String, Option<String>) =
            connection
                .query_row(
                    r#"
                    SELECT quantity_text, origin, note
                    FROM holding_snapshot
                    WHERE account_id = ?1 AND instrument_id = ?2 AND snapshot_date = '2099-03-01' AND origin = 'MANUAL'
                    ORDER BY holding_snapshot_id DESC
                    LIMIT 1
                    "#,
                    params![account_id, instrument_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .expect("manual row inserted");
        assert_eq!(manual_quantity, "123");
        assert_eq!(manual_origin, "MANUAL");
        assert_eq!(manual_note.as_deref(), Some("manual note"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn rejects_holding_state_save_for_older_than_latest_snapshot_date() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        fs::copy("assets/data.sqlite", &database_path).expect("copy seed db");

        let mut connection = Connection::open(&database_path).expect("open temp db");
        migrate(&mut connection).expect("migrate temp db");

        let (account_id, instrument_id, currency_code, latest_date): (i64, i64, String, String) =
            connection
                .query_row(
                    r#"
                    SELECT account_id, instrument_id, trading_currency_code, snapshot_date
                    FROM v_holding_metrics
                    ORDER BY market_value DESC, holding_snapshot_id DESC
                    LIMIT 1
                    "#,
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .expect("seed holding row");

        let input = CurrentHoldingStateInput {
            account_id,
            instrument_id,
            currency_code,
            as_of_date: "2000-01-01".to_string(),
            quantity_text: "1".to_string(),
            average_cost_text: "1".to_string(),
            note: String::new(),
        };

        let error = save_current_holding_state_with_connection(&mut connection, input)
            .expect_err("older date should fail");
        assert!(matches!(error, AppError::Validation(message) if message.contains(&latest_date)));
    }
}
