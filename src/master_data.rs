#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{params, Connection, OptionalExtension};

#[cfg(not(target_arch = "wasm32"))]
use crate::db::open_manual_write_database;
use crate::error::{AppError, AppResult};

#[derive(Clone, Debug, PartialEq)]
pub struct InstitutionOption {
    pub institution_id: i64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PersonOption {
    pub person_id: i64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AccountMasterRow {
    pub account_id: i64,
    pub display_name: String,
    pub institution_id: Option<i64>,
    pub institution_name: String,
    pub account_number: Option<String>,
    pub account_type: String,
    pub owner_id: Option<i64>,
    pub owner_name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InstrumentMasterRow {
    pub instrument_id: i64,
    pub symbol: String,
    pub name: String,
    pub instrument_type: String,
    pub asset_class: String,
    pub region_type: String,
    pub trading_currency_code: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AccountCreateInput {
    pub institution_id: i64,
    pub display_name: String,
    pub account_number: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AccountUpdateInput {
    pub account_id: i64,
    pub institution_id: i64,
    pub display_name: String,
    pub account_number: String,
    pub account_type: String,
    pub owner_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InstrumentCreateInput {
    pub symbol: String,
    pub name: String,
    pub instrument_type: String,
    pub asset_class: String,
    pub region_type: String,
    pub trading_currency_code: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InstrumentUpdateInput {
    pub instrument_id: i64,
    pub symbol: String,
    pub name: String,
    pub instrument_type: String,
    pub asset_class: String,
    pub region_type: String,
    pub trading_currency_code: String,
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_institution_options() -> Result<Vec<InstitutionOption>, String> {
    load_institution_options_native().map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_person_options() -> Result<Vec<PersonOption>, String> {
    load_person_options_native().map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_account_master_rows() -> Result<Vec<AccountMasterRow>, String> {
    load_account_master_rows_native().map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_instrument_master_rows() -> Result<Vec<InstrumentMasterRow>, String> {
    load_instrument_master_rows_native().map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_currency_codes() -> Result<Vec<String>, String> {
    let connection = crate::db::open_database().map_err(|error| error.to_string())?;
    let mut statement = connection
        .prepare("SELECT currency_code FROM currency ORDER BY currency_code ASC")
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| row.get(0))
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    Ok(rows)
}

#[cfg(target_arch = "wasm32")]
pub fn load_institution_options() -> Result<Vec<InstitutionOption>, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_person_options() -> Result<Vec<PersonOption>, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_account_master_rows() -> Result<Vec<AccountMasterRow>, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_instrument_master_rows() -> Result<Vec<InstrumentMasterRow>, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_currency_codes() -> Result<Vec<String>, String> {
    Err("SQLite 讀取目前只支援桌面版；Web 版需改由 server function 提供資料。".to_string())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn create_manual_account(input: AccountCreateInput) -> AppResult<i64> {
    let mut connection = open_manual_write_database()?;
    create_manual_account_with_connection(&mut connection, input)
}

#[cfg(target_arch = "wasm32")]
pub fn create_manual_account(_input: AccountCreateInput) -> AppResult<i64> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 帳戶新增".to_string(),
    ))
}

#[cfg(not(target_arch = "wasm32"))]
pub fn update_manual_account(input: AccountUpdateInput) -> AppResult<()> {
    let mut connection = open_manual_write_database()?;
    update_manual_account_with_connection(&mut connection, input)
}

#[cfg(target_arch = "wasm32")]
pub fn update_manual_account(_input: AccountUpdateInput) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 帳戶更新".to_string(),
    ))
}

#[cfg(not(target_arch = "wasm32"))]
pub fn create_manual_instrument(input: InstrumentCreateInput) -> AppResult<i64> {
    let mut connection = open_manual_write_database()?;
    create_manual_instrument_with_connection(&mut connection, input)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn update_manual_instrument(input: InstrumentUpdateInput) -> AppResult<()> {
    let mut connection = open_manual_write_database()?;
    update_manual_instrument_with_connection(&mut connection, input)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn delete_manual_account(account_id: i64) -> AppResult<()> {
    let mut connection = open_manual_write_database()?;
    delete_manual_account_with_connection(&mut connection, account_id)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn delete_manual_instrument(instrument_id: i64) -> AppResult<()> {
    let mut connection = open_manual_write_database()?;
    delete_manual_instrument_with_connection(&mut connection, instrument_id)
}

#[cfg(target_arch = "wasm32")]
pub fn create_manual_instrument(_input: InstrumentCreateInput) -> AppResult<i64> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 商品新增".to_string(),
    ))
}

#[cfg(target_arch = "wasm32")]
pub fn update_manual_instrument(_input: InstrumentUpdateInput) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 商品更新".to_string(),
    ))
}

#[cfg(target_arch = "wasm32")]
pub fn delete_manual_account(_account_id: i64) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 帳戶刪除".to_string(),
    ))
}

#[cfg(target_arch = "wasm32")]
pub fn delete_manual_instrument(_instrument_id: i64) -> AppResult<()> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 商品刪除".to_string(),
    ))
}

fn validate_account_create_input(input: &AccountCreateInput) -> AppResult<AccountCreateInput> {
    if input.institution_id <= 0 {
        return Err(AppError::Validation("請選擇金融機構".to_string()));
    }

    let display_name = input.display_name.trim().to_string();
    if display_name.is_empty() {
        return Err(AppError::Validation("請輸入帳戶名稱".to_string()));
    }

    let account_number = input.account_number.trim().to_string();
    if account_number.is_empty() {
        return Err(AppError::Validation("請輸入帳戶號碼".to_string()));
    }
    if !account_number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(AppError::Validation("帳戶號碼只能包含數字".to_string()));
    }

    Ok(AccountCreateInput {
        institution_id: input.institution_id,
        display_name,
        account_number,
    })
}

fn validate_account_update_input(input: &AccountUpdateInput) -> AppResult<AccountUpdateInput> {
    if input.account_id <= 0 {
        return Err(AppError::Validation("找不到帳戶".to_string()));
    }

    let created = validate_account_create_input(&AccountCreateInput {
        institution_id: input.institution_id,
        display_name: input.display_name.clone(),
        account_number: input.account_number.clone(),
    })?;
    let account_type = input.account_type.trim().to_string();
    if !matches!(account_type.as_str(), "BANK" | "BROKERAGE") {
        return Err(AppError::Validation("帳戶類型不正確".to_string()));
    }
    if input.owner_id <= 0 {
        return Err(AppError::Validation("請選擇所有權人".to_string()));
    }

    Ok(AccountUpdateInput {
        account_id: input.account_id,
        institution_id: created.institution_id,
        display_name: created.display_name,
        account_number: created.account_number,
        account_type,
        owner_id: input.owner_id,
    })
}

fn validate_instrument_create_input(
    input: &InstrumentCreateInput,
) -> AppResult<InstrumentCreateInput> {
    let symbol = input.symbol.trim().to_ascii_uppercase();
    let instrument_type = input.instrument_type.trim().to_string();
    if !matches!(
        instrument_type.as_str(),
        "STOCK" | "ETF" | "BOND" | "FUND" | "OTHER"
    ) {
        return Err(AppError::Validation("商品類型不正確".to_string()));
    }
    if matches!(instrument_type.as_str(), "STOCK" | "ETF") && symbol.is_empty() {
        return Err(AppError::Validation(
            "股票與 ETF 必須輸入商品代號".to_string(),
        ));
    }
    let asset_class = input.asset_class.trim().to_string();
    if !matches!(
        asset_class.as_str(),
        "EQUITY" | "BOND" | "MIXED" | "CASH_EQUIVALENT" | "OTHER"
    ) {
        return Err(AppError::Validation("資產類別不正確".to_string()));
    }
    let region_type = input.region_type.trim().to_string();
    if !matches!(region_type.as_str(), "DOMESTIC" | "FOREIGN") {
        return Err(AppError::Validation("區域不正確".to_string()));
    }

    let name = input.name.trim().to_string();
    if name.is_empty() {
        return Err(AppError::Validation("請輸入商品名稱".to_string()));
    }

    let trading_currency_code = input.trading_currency_code.trim().to_string();
    if trading_currency_code.is_empty() {
        return Err(AppError::Validation("請選擇交易幣別".to_string()));
    }

    Ok(InstrumentCreateInput {
        symbol,
        name,
        instrument_type,
        asset_class,
        region_type,
        trading_currency_code,
    })
}

fn validate_instrument_update_input(
    input: &InstrumentUpdateInput,
) -> AppResult<InstrumentUpdateInput> {
    if input.instrument_id <= 0 {
        return Err(AppError::Validation("找不到商品".to_string()));
    }
    let created = validate_instrument_create_input(&InstrumentCreateInput {
        symbol: input.symbol.clone(),
        name: input.name.clone(),
        instrument_type: input.instrument_type.clone(),
        asset_class: input.asset_class.clone(),
        region_type: input.region_type.clone(),
        trading_currency_code: input.trading_currency_code.clone(),
    })?;
    Ok(InstrumentUpdateInput {
        instrument_id: input.instrument_id,
        symbol: created.symbol,
        name: created.name,
        instrument_type: created.instrument_type,
        asset_class: created.asset_class,
        region_type: created.region_type,
        trading_currency_code: created.trading_currency_code,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn create_manual_account_with_connection(
    connection: &mut Connection,
    input: AccountCreateInput,
) -> AppResult<i64> {
    let validated = validate_account_create_input(&input)?;
    let transaction = connection.transaction()?;
    ensure_institution_exists(&transaction, validated.institution_id)?;
    if !validated.account_number.is_empty() {
        let existing_account: Option<i64> = transaction
            .query_row(
                "SELECT account_id FROM account WHERE institution_id = ?1 AND account_number = ?2 LIMIT 1",
                params![validated.institution_id, validated.account_number],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(account_id) = existing_account {
            return Err(AppError::Validation(format!(
                "此金融機構的帳戶號碼已存在於帳戶 #{account_id}"
            )));
        }
    }

    transaction.execute(
        r#"
        INSERT INTO account (display_name, institution_id, account_type, account_number, account_number_last4)
        VALUES (?1, ?2, 'BROKERAGE', NULLIF(?3, ''), NULLIF(SUBSTR(?3, -4), ''))
        "#,
        params![
            validated.display_name,
            validated.institution_id,
            validated.account_number
        ],
    )?;

    let account_id = transaction.last_insert_rowid();
    transaction.commit()?;
    Ok(account_id)
}

#[cfg(not(target_arch = "wasm32"))]
fn update_manual_account_with_connection(
    connection: &mut Connection,
    input: AccountUpdateInput,
) -> AppResult<()> {
    let validated = validate_account_update_input(&input)?;
    let transaction = connection.transaction()?;
    ensure_account_exists(&transaction, validated.account_id)?;
    ensure_institution_exists(&transaction, validated.institution_id)?;
    ensure_person_exists(&transaction, validated.owner_id)?;

    let existing_account: Option<i64> = transaction
        .query_row(
            "SELECT account_id FROM account WHERE institution_id = ?1 AND account_number = ?2 AND account_id <> ?3 LIMIT 1",
            params![validated.institution_id, validated.account_number, validated.account_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(account_id) = existing_account {
        return Err(AppError::Validation(format!(
            "此金融機構的帳戶號碼已存在於帳戶 #{account_id}"
        )));
    }

    transaction.execute(
        r#"
        UPDATE account
        SET display_name = ?1,
            institution_id = ?2,
            account_type = ?3,
            account_number = ?4,
            account_number_last4 = SUBSTR(?4, -4)
        WHERE account_id = ?5
        "#,
        params![
            validated.display_name,
            validated.institution_id,
            validated.account_type,
            validated.account_number,
            validated.account_id,
        ],
    )?;
    transaction.execute(
        "DELETE FROM account_owner WHERE account_id = ?1",
        [validated.account_id],
    )?;
    transaction.execute(
        "INSERT INTO account_owner (account_id, person_id) VALUES (?1, ?2)",
        params![validated.account_id, validated.owner_id],
    )?;
    transaction.commit()?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn create_manual_instrument_with_connection(
    connection: &mut Connection,
    input: InstrumentCreateInput,
) -> AppResult<i64> {
    let validated = validate_instrument_create_input(&input)?;
    let transaction = connection.transaction()?;
    ensure_currency_exists(&transaction, &validated.trading_currency_code)?;
    if !validated.symbol.is_empty() {
        let existing: Option<(i64, String)> = transaction
            .query_row(
                "SELECT instrument_id, name FROM instrument WHERE UPPER(TRIM(symbol)) = ?1 LIMIT 1",
                [&validated.symbol],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((instrument_id, name)) = existing {
            return Err(AppError::Validation(format!(
                "商品代號 {} 已由商品 #{instrument_id}（{name}）使用",
                validated.symbol
            )));
        }
    }

    transaction.execute(
        r#"
        INSERT INTO instrument (
            symbol,
            name,
            instrument_type,
            asset_class,
            region_type,
            trading_currency_code
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        "#,
        params![
            validated.symbol,
            validated.name,
            validated.instrument_type,
            validated.asset_class,
            validated.region_type,
            validated.trading_currency_code,
        ],
    )?;

    let instrument_id = transaction.last_insert_rowid();
    transaction.commit()?;
    Ok(instrument_id)
}

#[cfg(not(target_arch = "wasm32"))]
fn update_manual_instrument_with_connection(
    connection: &mut Connection,
    input: InstrumentUpdateInput,
) -> AppResult<()> {
    let validated = validate_instrument_update_input(&input)?;
    let transaction = connection.transaction()?;
    ensure_instrument_exists(&transaction, validated.instrument_id)?;
    ensure_currency_exists(&transaction, &validated.trading_currency_code)?;
    if !validated.symbol.is_empty() {
        let existing: Option<(i64, String)> = transaction
            .query_row(
                "SELECT instrument_id, name FROM instrument WHERE UPPER(TRIM(symbol)) = ?1 AND instrument_id <> ?2 LIMIT 1",
                params![validated.symbol, validated.instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((instrument_id, name)) = existing {
            return Err(AppError::Validation(format!(
                "商品代號 {} 已由商品 #{instrument_id}（{name}）使用",
                validated.symbol
            )));
        }
    }
    transaction.execute(
        "UPDATE instrument SET symbol = ?1, name = ?2, instrument_type = ?3, asset_class = ?4, region_type = ?5, trading_currency_code = ?6 WHERE instrument_id = ?7",
        params![validated.symbol, validated.name, validated.instrument_type, validated.asset_class, validated.region_type, validated.trading_currency_code, validated.instrument_id],
    )?;
    transaction.commit()?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn delete_manual_account_with_connection(
    connection: &mut Connection,
    account_id: i64,
) -> AppResult<()> {
    if account_id <= 0 {
        return Err(AppError::Validation("找不到帳戶".to_string()));
    }
    let transaction = connection.transaction()?;
    ensure_account_exists(&transaction, account_id)?;
    ensure_unreferenced(
        &transaction,
        "account_asset_snapshot",
        "account_id",
        account_id,
        "帳戶",
    )?;
    ensure_unreferenced(
        &transaction,
        "holding_snapshot",
        "account_id",
        account_id,
        "帳戶",
    )?;
    ensure_unreferenced(
        &transaction,
        "dividend_receipt",
        "account_id",
        account_id,
        "帳戶",
    )?;
    transaction.execute("DELETE FROM account WHERE account_id = ?1", [account_id])?;
    transaction.commit()?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn delete_manual_instrument_with_connection(
    connection: &mut Connection,
    instrument_id: i64,
) -> AppResult<()> {
    if instrument_id <= 0 {
        return Err(AppError::Validation("找不到商品".to_string()));
    }
    let transaction = connection.transaction()?;
    ensure_instrument_exists(&transaction, instrument_id)?;
    for table in [
        "holding_snapshot",
        "instrument_price",
        "dividend_receipt",
        "dividend_assumption",
        "instrument_annual_dividend",
        "dividend_legacy_monthly",
        "dividend_legacy_summary",
    ] {
        ensure_unreferenced(&transaction, table, "instrument_id", instrument_id, "商品")?;
    }
    transaction.execute(
        "DELETE FROM instrument WHERE instrument_id = ?1",
        [instrument_id],
    )?;
    transaction.commit()?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn load_institution_options_native() -> rusqlite::Result<Vec<InstitutionOption>> {
    let connection = crate::db::open_database()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let mut statement = connection.prepare(
        r#"
        SELECT institution_id, COALESCE(name, '未命名機構') AS name
        FROM institution
        ORDER BY name ASC
        "#,
    )?;

    let rows = statement.query_map([], |row| {
        Ok(InstitutionOption {
            institution_id: row.get(0)?,
            name: row.get(1)?,
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn load_person_options_native() -> rusqlite::Result<Vec<PersonOption>> {
    let connection = crate::db::open_database()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let mut statement = connection.prepare(
        "SELECT person_id, COALESCE(display_name, '未命名') FROM person ORDER BY display_name ASC, person_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(PersonOption {
            person_id: row.get(0)?,
            name: row.get(1)?,
        })
    })?;

    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn load_account_master_rows_native() -> rusqlite::Result<Vec<AccountMasterRow>> {
    let connection = crate::db::open_database()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let mut statement = connection.prepare(
        "SELECT a.account_id, a.display_name, a.institution_id, COALESCE(i.name, '未指定金融機構'), a.account_number, a.account_type, o.person_id, COALESCE(p.display_name, '未指定所有權人') FROM account a LEFT JOIN institution i ON i.institution_id = a.institution_id LEFT JOIN account_owner o ON o.account_id = a.account_id LEFT JOIN person p ON p.person_id = o.person_id ORDER BY a.display_name ASC, a.account_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(AccountMasterRow {
            account_id: row.get(0)?,
            display_name: row.get(1)?,
            institution_id: row.get(2)?,
            institution_name: row.get(3)?,
            account_number: row.get(4)?,
            account_type: row.get(5)?,
            owner_id: row.get(6)?,
            owner_name: row.get(7)?,
        })
    })?;
    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn load_instrument_master_rows_native() -> rusqlite::Result<Vec<InstrumentMasterRow>> {
    let connection = crate::db::open_database()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let mut statement = connection.prepare(
        "SELECT instrument_id, COALESCE(symbol, ''), name, instrument_type, asset_class, COALESCE(region_type, ''), trading_currency_code FROM instrument ORDER BY name ASC, instrument_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(InstrumentMasterRow {
            instrument_id: row.get(0)?,
            symbol: row.get(1)?,
            name: row.get(2)?,
            instrument_type: row.get(3)?,
            asset_class: row.get(4)?,
            region_type: row.get(5)?,
            trading_currency_code: row.get(6)?,
        })
    })?;
    rows.collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_institution_exists(connection: &Connection, institution_id: i64) -> AppResult<()> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT institution_id FROM institution WHERE institution_id = ?1 LIMIT 1",
            [institution_id],
            |row| row.get(0),
        )
        .optional()?;

    if exists.is_some() {
        Ok(())
    } else {
        Err(AppError::Validation(format!(
            "找不到金融機構：{institution_id}"
        )))
    }
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
fn ensure_unreferenced(
    connection: &Connection,
    table: &str,
    column: &str,
    id: i64,
    label: &str,
) -> AppResult<()> {
    let exists: Option<i64> = connection
        .query_row(
            &format!("SELECT 1 FROM {table} WHERE {column} = ?1 LIMIT 1"),
            [id],
            |row| row.get(0),
        )
        .optional()?;
    if exists.is_some() {
        Err(AppError::Validation(format!(
            "此{label}已有歷史資料，無法刪除"
        )))
    } else {
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_person_exists(connection: &Connection, person_id: i64) -> AppResult<()> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT person_id FROM person WHERE person_id = ?1 LIMIT 1",
            [person_id],
            |row| row.get(0),
        )
        .optional()?;

    if exists.is_some() {
        Ok(())
    } else {
        Err(AppError::Validation(format!("找不到所有權人：{person_id}")))
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

    #[cfg(not(target_arch = "wasm32"))]
    fn seed_db(connection: &mut Connection) {
        connection
            .execute_batch(
                r#"
                CREATE TABLE institution (
                    institution_id INTEGER PRIMARY KEY,
                    name TEXT
                );

                CREATE TABLE account (
                    account_id INTEGER PRIMARY KEY,
                    display_name TEXT,
                    institution_id INTEGER,
                    account_type TEXT,
                    account_number TEXT,
                    account_number_last4 TEXT
                );

                CREATE TABLE person (
                    person_id INTEGER PRIMARY KEY,
                    display_name TEXT
                );

                CREATE TABLE account_owner (
                    account_id INTEGER PRIMARY KEY,
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

                CREATE TABLE currency (
                    currency_code TEXT PRIMARY KEY
                );

                CREATE TABLE account_asset_snapshot (account_id INTEGER);
                CREATE TABLE holding_snapshot (account_id INTEGER, instrument_id INTEGER);
                CREATE TABLE dividend_receipt (account_id INTEGER, instrument_id INTEGER);
                CREATE TABLE dividend_assumption (instrument_id INTEGER);
                CREATE TABLE instrument_annual_dividend (instrument_id INTEGER);
                CREATE TABLE instrument_price (instrument_id INTEGER);
                CREATE TABLE dividend_legacy_monthly (instrument_id INTEGER);
                CREATE TABLE dividend_legacy_summary (instrument_id INTEGER);

                INSERT INTO institution (institution_id, name) VALUES (1, 'Demo Bank');
                INSERT INTO institution (institution_id, name) VALUES (2, 'Other Bank');
                INSERT INTO person (person_id, display_name) VALUES (1, '原所有權人');
                INSERT INTO person (person_id, display_name) VALUES (2, '新所有權人');
                INSERT INTO currency (currency_code) VALUES ('NTD');
                "#,
            )
            .expect("seed db");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn create_manual_account_inserts_row() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_db(&mut connection);

        let account_id = create_manual_account_with_connection(
            &mut connection,
            AccountCreateInput {
                institution_id: 1,
                display_name: "新帳戶".to_string(),
                account_number: "001234567890".to_string(),
            },
        )
        .expect("create account");

        let (display_name, account_number, account_number_last4):
            (String, Option<String>, Option<String>) = connection
            .query_row(
                "SELECT display_name, account_number, account_number_last4 FROM account WHERE account_id = ?1",
                [account_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("read account");

        assert_eq!(display_name, "新帳戶");
        assert_eq!(account_number.as_deref(), Some("001234567890"));
        assert_eq!(account_number_last4.as_deref(), Some("7890"));
    }

    #[test]
    fn rejects_a_manual_account_number_with_non_digits() {
        let error = validate_account_create_input(&AccountCreateInput {
            institution_id: 1,
            display_name: "新帳戶".to_string(),
            account_number: "1234-5678".to_string(),
        })
        .expect_err("account number should be rejected");

        assert!(error.to_string().contains("帳戶號碼只能包含數字"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn update_manual_account_replaces_its_owner_and_master_data() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_db(&mut connection);
        connection
            .execute(
                "INSERT INTO account (account_id, display_name, institution_id, account_type, account_number, account_number_last4) VALUES (1, '舊帳戶', 1, 'BANK', '00001234', '1234')",
                [],
            )
            .expect("insert account");
        connection
            .execute(
                "INSERT INTO account_owner (account_id, person_id) VALUES (1, 1)",
                [],
            )
            .expect("insert owner");

        update_manual_account_with_connection(
            &mut connection,
            AccountUpdateInput {
                account_id: 1,
                institution_id: 2,
                display_name: "新帳戶".to_string(),
                account_number: "99887766".to_string(),
                account_type: "BROKERAGE".to_string(),
                owner_id: 2,
            },
        )
        .expect("update account");

        let account: (String, i64, String, String, String) = connection
            .query_row(
                "SELECT display_name, institution_id, account_type, account_number, account_number_last4 FROM account WHERE account_id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .expect("read account");
        let owner_id: i64 = connection
            .query_row(
                "SELECT person_id FROM account_owner WHERE account_id = 1",
                [],
                |row| row.get(0),
            )
            .expect("read owner");

        assert_eq!(
            account,
            (
                "新帳戶".to_string(),
                2,
                "BROKERAGE".to_string(),
                "99887766".to_string(),
                "7766".to_string()
            )
        );
        assert_eq!(owner_id, 2);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn update_manual_account_rejects_an_existing_institution_account_number() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_db(&mut connection);
        connection
            .execute(
                "INSERT INTO account (account_id, display_name, institution_id, account_type, account_number, account_number_last4) VALUES (1, '帳戶一', 1, 'BANK', '00001234', '1234'), (2, '帳戶二', 1, 'BANK', '99887766', '7766')",
                [],
            )
            .expect("insert accounts");

        let error = update_manual_account_with_connection(
            &mut connection,
            AccountUpdateInput {
                account_id: 1,
                institution_id: 1,
                display_name: "帳戶一".to_string(),
                account_number: "99887766".to_string(),
                account_type: "BANK".to_string(),
                owner_id: 1,
            },
        )
        .expect_err("duplicate account number should fail");

        assert!(error.to_string().contains("帳戶號碼已存在"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn create_manual_instrument_inserts_row() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_db(&mut connection);

        let instrument_id = create_manual_instrument_with_connection(
            &mut connection,
            InstrumentCreateInput {
                symbol: "ABC".to_string(),
                name: "測試商品".to_string(),
                instrument_type: "ETF".to_string(),
                asset_class: "EQUITY".to_string(),
                region_type: "DOMESTIC".to_string(),
                trading_currency_code: "NTD".to_string(),
            },
        )
        .expect("create instrument");

        let (symbol, currency_code): (String, String) = connection
            .query_row(
                "SELECT symbol, trading_currency_code FROM instrument WHERE instrument_id = ?1",
                [instrument_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read instrument");

        assert_eq!(symbol, "ABC");
        assert_eq!(currency_code, "NTD");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn update_manual_instrument_replaces_master_data() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_db(&mut connection);
        connection
            .execute(
                "INSERT INTO instrument (instrument_id, symbol, name, instrument_type, asset_class, region_type, trading_currency_code) VALUES (1, 'OLD', '舊商品', 'ETF', 'EQUITY', 'DOMESTIC', 'NTD')",
                [],
            )
            .expect("insert instrument");

        update_manual_instrument_with_connection(
            &mut connection,
            InstrumentUpdateInput {
                instrument_id: 1,
                symbol: "new".to_string(),
                name: "新商品".to_string(),
                instrument_type: "FUND".to_string(),
                asset_class: "BOND".to_string(),
                region_type: "FOREIGN".to_string(),
                trading_currency_code: "NTD".to_string(),
            },
        )
        .expect("update instrument");

        let row: (String, String, String) = connection
            .query_row(
                "SELECT symbol, name, instrument_type FROM instrument WHERE instrument_id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("read instrument");
        assert_eq!(
            row,
            ("NEW".to_string(), "新商品".to_string(), "FUND".to_string())
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn deletes_only_unreferenced_master_data() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_db(&mut connection);
        connection
            .execute(
                "INSERT INTO account (account_id, display_name, institution_id, account_type, account_number) VALUES (1, '可刪除帳戶', 1, 'BANK', '1234')",
                [],
            )
            .expect("insert account");
        connection
            .execute(
                "INSERT INTO instrument (instrument_id, symbol, name, instrument_type, asset_class, region_type, trading_currency_code) VALUES (1, 'DEL', '可刪除商品', 'ETF', 'EQUITY', 'DOMESTIC', 'NTD')",
                [],
            )
            .expect("insert instrument");

        delete_manual_account_with_connection(&mut connection, 1).expect("delete account");
        delete_manual_instrument_with_connection(&mut connection, 1).expect("delete instrument");

        let account_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM account", [], |row| row.get(0))
            .expect("count accounts");
        let instrument_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM instrument", [], |row| row.get(0))
            .expect("count instruments");
        assert_eq!(account_count, 0);
        assert_eq!(instrument_count, 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn refuses_to_delete_referenced_master_data() {
        let mut connection = Connection::open_in_memory().expect("open db");
        seed_db(&mut connection);
        connection
            .execute(
                "INSERT INTO account (account_id, display_name, institution_id, account_type, account_number) VALUES (1, '使用中帳戶', 1, 'BANK', '1234')",
                [],
            )
            .expect("insert account");
        connection
            .execute(
                "INSERT INTO account_asset_snapshot (account_id) VALUES (1)",
                [],
            )
            .expect("insert asset");
        let error = delete_manual_account_with_connection(&mut connection, 1)
            .expect_err("referenced account must not delete");
        assert!(error.to_string().contains("已有歷史資料"));
    }

    #[test]
    fn normalizes_stock_symbol_and_allows_a_fund_without_one() {
        let stock = validate_instrument_create_input(&InstrumentCreateInput {
            symbol: " aapl ".to_string(),
            name: "Apple".to_string(),
            instrument_type: "STOCK".to_string(),
            asset_class: "EQUITY".to_string(),
            region_type: "FOREIGN".to_string(),
            trading_currency_code: "USD".to_string(),
        })
        .expect("valid stock");
        assert_eq!(stock.symbol, "AAPL");

        let fund = validate_instrument_create_input(&InstrumentCreateInput {
            symbol: String::new(),
            name: "測試基金".to_string(),
            instrument_type: "FUND".to_string(),
            asset_class: "BOND".to_string(),
            region_type: "FOREIGN".to_string(),
            trading_currency_code: "NTD".to_string(),
        })
        .expect("fund may omit a symbol");
        assert!(fund.symbol.is_empty());
    }

    #[test]
    fn rejects_stock_without_a_symbol() {
        let error = validate_instrument_create_input(&InstrumentCreateInput {
            symbol: String::new(),
            name: "測試股票".to_string(),
            instrument_type: "STOCK".to_string(),
            asset_class: "EQUITY".to_string(),
            region_type: "DOMESTIC".to_string(),
            trading_currency_code: "NTD".to_string(),
        })
        .expect_err("stock requires a symbol");
        assert!(matches!(error, AppError::Validation(_)));
    }
}
