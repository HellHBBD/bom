use std::collections::{HashMap, HashSet};

#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Mutex, OnceLock};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

use dioxus::prelude::*;

#[cfg(not(target_arch = "wasm32"))]
use crate::db::{open_database, open_ui_preference_database};

pub const LAST_ROUTE: &str = "layout.last_route";
pub const ACCOUNTS_SEARCH: &str = "accounts.search";
pub const ACCOUNTS_OWNER: &str = "accounts.owner";
pub const ACCOUNTS_INSTITUTION: &str = "accounts.institution";
pub const ACCOUNTS_ASSET_TYPE: &str = "accounts.asset_type";
pub const ACCOUNTS_CURRENCY: &str = "accounts.currency";
pub const ACCOUNTS_SORT: &str = "accounts.sort";
pub const HOLDINGS_SEARCH: &str = "holdings.search";
pub const HOLDINGS_OWNER: &str = "holdings.owner";
pub const HOLDINGS_TYPE: &str = "holdings.type";
pub const HOLDINGS_ASSET_CLASS: &str = "holdings.asset_class";
pub const HOLDINGS_REGION: &str = "holdings.region";
pub const HOLDINGS_SORT: &str = "holdings.sort";
pub const HOLDINGS_VISIBLE_COLUMNS: &str = "holdings.visible_columns";
pub const QUICK_PRICE_SEARCH: &str = "quick_price.search";
pub const QUICK_PRICE_CURRENCY: &str = "quick_price.currency";
pub const QUICK_PRICE_SORT: &str = "quick_price.sort";
pub const QUICK_PRICE_DATE: &str = "quick_price.date";
pub const LEGACY_DIVIDENDS_SEARCH: &str = "legacy_dividends.search";
pub const LEGACY_DIVIDENDS_OWNER: &str = "legacy_dividends.owner";
pub const LEGACY_DIVIDENDS_INSTRUMENT: &str = "legacy_dividends.instrument";
pub const LEGACY_DIVIDENDS_PERIOD: &str = "legacy_dividends.period";
pub const LEGACY_DIVIDENDS_SORT: &str = "legacy_dividends.sort";

pub type UiPreferences = Signal<HashMap<String, String>>;

#[cfg(not(target_arch = "wasm32"))]
static PENDING_PREFERENCES: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
#[cfg(not(target_arch = "wasm32"))]
static PREFERENCE_WRITE_LOCK: Mutex<()> = Mutex::new(());

#[cfg(not(target_arch = "wasm32"))]
fn pending_preferences() -> &'static Mutex<HashMap<String, String>> {
    PENDING_PREFERENCES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_all_preferences() -> Result<HashMap<String, String>, String> {
    let connection = open_database().map_err(|error| error.to_string())?;
    load_all_preferences_from(&connection).map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
fn load_all_preferences_from(
    connection: &rusqlite::Connection,
) -> rusqlite::Result<HashMap<String, String>> {
    let mut statement =
        connection.prepare("SELECT preference_key, value_text FROM ui_preference")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    rows.collect()
}

#[cfg(target_arch = "wasm32")]
pub fn load_all_preferences() -> Result<HashMap<String, String>, String> {
    Ok(HashMap::new())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn upsert_preference(preference_key: &str, value_text: &str) -> Result<(), String> {
    let connection = open_ui_preference_database().map_err(|error| error.to_string())?;
    upsert_preference_into(&connection, preference_key, value_text)
        .map_err(|error| error.to_string())
}

#[cfg(not(target_arch = "wasm32"))]
fn upsert_preference_into(
    connection: &rusqlite::Connection,
    preference_key: &str,
    value_text: &str,
) -> rusqlite::Result<()> {
    connection
        .execute(
            r#"
            INSERT INTO ui_preference (preference_key, value_text)
            VALUES (?1, ?2)
            ON CONFLICT(preference_key) DO UPDATE SET value_text = excluded.value_text
            "#,
            rusqlite::params![preference_key, value_text],
        )
        .map(|_| ())
}

#[cfg(target_arch = "wasm32")]
pub fn upsert_preference(_preference_key: &str, _value_text: &str) -> Result<(), String> {
    Ok(())
}

pub fn preference_value(preferences: &HashMap<String, String>, key: &str) -> String {
    preferences.get(key).cloned().unwrap_or_default()
}

pub fn valid_option(value: &str, options: &[String], default: &str) -> String {
    if value.is_empty() || options.iter().any(|option| option == value) {
        value.to_string()
    } else {
        default.to_string()
    }
}

pub fn valid_sort(value: &str, allowed: &[&str], default: &str) -> String {
    if allowed.contains(&value) {
        value.to_string()
    } else {
        default.to_string()
    }
}

pub fn parse_visible_columns(value: &str, known_ids: &[&str]) -> HashSet<String> {
    if value == "-" {
        return HashSet::new();
    }
    let visible = value
        .split(',')
        .filter(|column| known_ids.contains(column))
        .map(ToString::to_string)
        .collect::<HashSet<_>>();
    if visible.is_empty() {
        known_ids.iter().map(|id| (*id).to_string()).collect()
    } else {
        visible
    }
}

pub fn serialize_visible_columns(visible: &HashSet<String>, known_ids: &[&str]) -> String {
    let serialized = known_ids
        .iter()
        .filter(|id| visible.contains(**id))
        .copied()
        .collect::<Vec<_>>()
        .join(",");
    if serialized.is_empty() {
        "-".to_string()
    } else {
        serialized
    }
}

pub fn persist_preference(mut preferences: UiPreferences, key: &'static str, value: String) {
    if preferences()
        .get(key)
        .is_some_and(|current_value| current_value == &value)
    {
        return;
    }

    preferences.with_mut(|values| {
        values.insert(key.to_string(), value.clone());
    });

    #[cfg(not(target_arch = "wasm32"))]
    {
        let key = key.to_string();
        if let Ok(mut pending) = pending_preferences().lock() {
            pending.insert(key.clone(), value.clone());
        } else {
            eprintln!("無法取得 UI 偏好設定寫入鎖");
            return;
        }

        spawn(async move {
            let mut retry_delay = Duration::from_millis(250);
            loop {
                tokio::time::sleep(retry_delay).await;
                let is_current = match pending_preferences().lock() {
                    Ok(pending) => pending.get(&key) == Some(&value),
                    Err(_) => {
                        eprintln!("無法取得 UI 偏好設定寫入鎖");
                        return;
                    }
                };
                if !is_current {
                    return;
                }

                let write_key = key.clone();
                let value_to_write = value.clone();
                match tokio::task::spawn_blocking(move || {
                    let _write_lock = PREFERENCE_WRITE_LOCK
                        .lock()
                        .map_err(|_| "無法取得 UI 偏好設定寫入鎖".to_string())?;
                    let pending = pending_preferences()
                        .lock()
                        .map_err(|_| "無法取得 UI 偏好設定寫入鎖".to_string())?;
                    if pending.get(&write_key) != Some(&value_to_write) {
                        return Ok(());
                    }
                    drop(pending);

                    let mut last_error = String::new();
                    for attempt in 0..3 {
                        match upsert_preference(&write_key, &value_to_write) {
                            Ok(()) => return Ok(()),
                            Err(error) => {
                                last_error = error;
                                if attempt < 2 {
                                    std::thread::sleep(Duration::from_millis(100));
                                }
                            }
                        }
                    }
                    Err(last_error)
                })
                .await
                {
                    Ok(Ok(())) => {
                        if let Ok(mut pending) = pending_preferences().lock() {
                            if pending.get(&key) == Some(&value) {
                                pending.remove(&key);
                            }
                        }
                        return;
                    }
                    Ok(Err(error)) => eprintln!("無法儲存 UI 偏好設定 {key}：{error}"),
                    Err(error) => eprintln!("UI 偏好設定寫入工作失敗：{error}"),
                }
                retry_delay = Duration::from_secs(1);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{parse_visible_columns, serialize_visible_columns, valid_option, valid_sort};

    #[cfg(not(target_arch = "wasm32"))]
    use super::{load_all_preferences_from, upsert_preference_into};

    #[test]
    fn invalid_dynamic_option_and_sort_use_defaults() {
        let options = vec!["Alex".to_string(), "Beth".to_string()];
        assert_eq!(valid_option("Chris", &options, ""), "");
        assert_eq!(valid_option("Alex", &options, ""), "Alex");
        assert_eq!(
            valid_sort("unknown", &["owner", "amount"], "owner"),
            "owner"
        );
    }

    #[test]
    fn visible_columns_keep_known_ids_and_default_when_empty() {
        let known = ["owner", "symbol", "market_value"];
        let visible = parse_visible_columns("owner,unknown,symbol", &known);
        assert_eq!(serialize_visible_columns(&visible, &known), "owner,symbol");

        let defaults = parse_visible_columns("unknown", &known);
        assert_eq!(
            serialize_visible_columns(&defaults, &known),
            "owner,symbol,market_value"
        );

        assert!(parse_visible_columns("-", &known).is_empty());
        assert_eq!(serialize_visible_columns(&HashSet::new(), &known), "-");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn upsert_replaces_existing_preference_value() {
        let connection = rusqlite::Connection::open_in_memory().expect("open database");
        connection
            .execute_batch(
                "CREATE TABLE ui_preference (preference_key TEXT PRIMARY KEY NOT NULL, value_text TEXT NOT NULL);",
            )
            .expect("create preference table");

        upsert_preference_into(&connection, "accounts.sort", "owner").expect("insert preference");
        upsert_preference_into(&connection, "accounts.sort", "value").expect("replace preference");

        assert_eq!(
            load_all_preferences_from(&connection).expect("load preferences"),
            std::collections::HashMap::from([("accounts.sort".to_string(), "value".to_string())])
        );
    }
}
