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
pub struct AccountCreateInput {
    pub institution_id: i64,
    pub display_name: String,
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

#[cfg(not(target_arch = "wasm32"))]
pub fn load_institution_options() -> Result<Vec<InstitutionOption>, String> {
    load_institution_options_native().map_err(|error| error.to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn load_institution_options() -> Result<Vec<InstitutionOption>, String> {
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
pub fn create_manual_instrument(input: InstrumentCreateInput) -> AppResult<i64> {
    let mut connection = open_manual_write_database()?;
    create_manual_instrument_with_connection(&mut connection, input)
}

#[cfg(target_arch = "wasm32")]
pub fn create_manual_instrument(_input: InstrumentCreateInput) -> AppResult<i64> {
    Err(AppError::Validation(
        "目前只支援桌面版 SQLite 商品新增".to_string(),
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

    Ok(AccountCreateInput {
        institution_id: input.institution_id,
        display_name,
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

#[cfg(not(target_arch = "wasm32"))]
fn create_manual_account_with_connection(
    connection: &mut Connection,
    input: AccountCreateInput,
) -> AppResult<i64> {
    let validated = validate_account_create_input(&input)?;
    let transaction = connection.transaction()?;
    ensure_institution_exists(&transaction, validated.institution_id)?;

    transaction.execute(
        r#"
        INSERT INTO account (display_name, institution_id, account_type)
        VALUES (?1, ?2, 'BROKERAGE')
        "#,
        params![validated.display_name, validated.institution_id],
    )?;

    let account_id = transaction.last_insert_rowid();
    transaction.commit()?;
    Ok(account_id)
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
                    account_type TEXT
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

                INSERT INTO institution (institution_id, name) VALUES (1, 'Demo Bank');
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
            },
        )
        .expect("create account");

        let display_name: String = connection
            .query_row(
                "SELECT display_name FROM account WHERE account_id = ?1",
                [account_id],
                |row| row.get(0),
            )
            .expect("read account");

        assert_eq!(display_name, "新帳戶");
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
