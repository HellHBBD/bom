use dioxus::prelude::*;

use crate::db::{
    load_account_assets, load_dashboard_summary, load_holding_metrics, load_legacy_dividends,
};
use crate::format::{decimal, money, percent};
use crate::models::{
    AccountAsset, DashboardSummary, HoldingMetric, LegacyDividendData, LegacyDividendMonthlyRow,
    LegacyDividendSummaryRow,
};

#[component]
pub fn DashboardPage() -> Element {
    let summary = use_resource(move || async move { load_dashboard_summary() });

    rsx! {
        PageHeader {
            title: "資產總覽".to_string(),
            description: "彙總最新帳戶資產、投資市值與預估配息。".to_string(),
        }

        match summary() {
            None => rsx! { StatusCard { text: "載入資產總覽中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取資產總覽失敗：{error}") } },
            Some(Ok(summary)) => rsx! { DashboardCards { summary } },
        }
    }
}

#[component]
pub fn AccountsPage() -> Element {
    let account_assets = use_resource(move || async move { load_account_assets() });

    rsx! {
        PageHeader {
            title: "帳戶資產".to_string(),
            description: "由 v_account_asset_value 讀取最新帳戶資產快照與台幣換算值。".to_string(),
        }

        match account_assets() {
            None => rsx! { StatusCard { text: "載入帳戶資產中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取帳戶資產失敗：{error}") } },
            Some(Ok(rows)) if rows.is_empty() => rsx! { StatusCard { text: "目前沒有帳戶資產資料。".to_string() } },
            Some(Ok(rows)) => rsx! { AccountAssetsTable { rows } },
        }
    }
}

#[component]
pub fn HoldingsPage() -> Element {
    let holdings = use_resource(move || async move { load_holding_metrics() });

    rsx! {
        PageHeader {
            title: "持股明細".to_string(),
            description: "由 v_holding_metrics 讀取最新持股、成本、市值、損益與預估配息。".to_string(),
        }

        match holdings() {
            None => rsx! { StatusCard { text: "載入持股資料中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取持股資料失敗：{error}") } },
            Some(Ok(rows)) if rows.is_empty() => rsx! { StatusCard { text: "目前沒有持股資料。".to_string() } },
            Some(Ok(rows)) => rsx! { HoldingsTable { rows } },
        }
    }
}

#[component]
pub fn DividendsLegacyPage() -> Element {
    let legacy_dividends = use_resource(move || async move { load_legacy_dividends() });

    rsx! {
        PageHeader {
            title: "Excel 歷史股息彙總".to_string(),
            description: "唯讀保存股息收入明細表中無法轉成逐筆股息紀錄的歷史彙總值。".to_string(),
        }

        section { class: "card legacy-note",
            strong { "唯讀歷史資料" }
            p { "這些資料保留原始 Excel 手動輸入的年度、累積與月份彙總，包含來源儲存格位置；不包含入帳日期、入帳帳戶、稅額、費用或除息資訊，因此不會轉成逐筆 dividend_receipt。" }
        }

        match legacy_dividends() {
            None => rsx! { StatusCard { text: "載入 Excel 歷史股息彙總中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取 Excel 歷史股息彙總失敗：{error}") } },
            Some(Ok(data)) if data.summaries.is_empty() && data.monthly.is_empty() => rsx! { StatusCard { text: "目前沒有 Excel 歷史股息彙總資料。".to_string() } },
            Some(Ok(data)) => rsx! { LegacyDividendTables { data } },
        }
    }
}

#[component]
fn PageHeader(title: String, description: String) -> Element {
    rsx! {
        header { class: "page-header",
            p { class: "eyebrow", "Read-only MVP" }
            h2 { "{title}" }
            p { class: "page-description", "{description}" }
        }
    }
}

#[component]
fn PlaceholderCard(text: String) -> Element {
    rsx! {
        section { class: "card placeholder", "{text}" }
    }
}

#[component]
fn StatusCard(text: String) -> Element {
    rsx! {
        section { class: "card status", "{text}" }
    }
}

#[component]
fn DashboardCards(summary: DashboardSummary) -> Element {
    let latest_account_asset_date = summary.latest_account_asset_date.as_deref().unwrap_or("-");
    let latest_holding_date = summary.latest_holding_date.as_deref().unwrap_or("-");

    rsx! {
        section { class: "dashboard-grid",
            MetricCard {
                label: "總資產".to_string(),
                value: money(summary.total_assets),
                hint: incomplete_total_hint(&summary),
                accent: "primary".to_string(),
            }
            MetricCard {
                label: "帳戶資產".to_string(),
                value: money(summary.account_assets),
                hint: missing_value_hint(
                    summary.account_asset_count,
                    summary.account_asset_missing_value_count,
                    "筆最新快照",
                    "筆缺台幣價值",
                ),
                accent: "cash".to_string(),
            }
            MetricCard {
                label: "投資資產".to_string(),
                value: money(summary.investment_assets),
                hint: missing_value_hint(
                    summary.holding_count,
                    summary.holding_missing_market_value_count,
                    "筆持股",
                    "筆缺市值",
                ),
                accent: "investment".to_string(),
            }
            MetricCard {
                label: "預估年配息".to_string(),
                value: money(summary.estimated_annual_dividend),
                hint: dividend_hint(summary.holding_count, summary.holding_missing_dividend_count),
                accent: "income".to_string(),
            }
            MetricCard {
                label: "預估月平均配息".to_string(),
                value: money(summary.estimated_monthly_dividend),
                hint: if summary.holding_missing_dividend_count == 0 {
                    "預估年配息 / 12".to_string()
                } else {
                    format!("預估年配息 / 12，{} 筆缺配息假設", summary.holding_missing_dividend_count)
                },
                accent: "income".to_string(),
            }
            section { class: "card dashboard-status",
                h3 { "資料狀態" }
                dl {
                    div {
                        dt { "帳戶資產筆數" }
                        dd { "{summary.account_asset_count} 筆 / 缺值 {summary.account_asset_missing_value_count} 筆" }
                    }
                    div {
                        dt { "持股筆數" }
                        dd { "{summary.holding_count} 筆 / 缺市值 {summary.holding_missing_market_value_count} 筆 / 缺配息 {summary.holding_missing_dividend_count} 筆" }
                    }
                    div {
                        dt { "最新帳戶資產日期" }
                        dd { class: "mono", "{latest_account_asset_date}" }
                    }
                    div {
                        dt { "最新持股日期" }
                        dd { class: "mono", "{latest_holding_date}" }
                    }
                }
            }
        }
    }
}

fn incomplete_total_hint(summary: &DashboardSummary) -> String {
    let missing_count =
        summary.account_asset_missing_value_count + summary.holding_missing_market_value_count;
    if missing_count == 0 {
        "帳戶資產 + 投資市值".to_string()
    } else {
        format!("帳戶資產 + 投資市值，另有 {missing_count} 筆缺值未計入")
    }
}

fn missing_value_hint(
    total_count: i64,
    missing_count: i64,
    total_label: &str,
    missing_label: &str,
) -> String {
    if missing_count == 0 {
        format!("{total_count} {total_label}")
    } else {
        format!("{total_count} {total_label}，{missing_count} {missing_label}")
    }
}

fn dividend_hint(total_count: i64, missing_count: i64) -> String {
    if missing_count == 0 {
        format!("由 {total_count} 筆持股配息假設加總")
    } else {
        format!("由持股配息假設加總，{missing_count} 筆缺配息假設未計入")
    }
}

#[component]
fn MetricCard(label: String, value: String, hint: String, accent: String) -> Element {
    rsx! {
        section { class: "card metric-card {accent}",
            p { class: "metric-label", "{label}" }
            strong { class: "metric-value", "{value}" }
            p { class: "metric-hint", "{hint}" }
        }
    }
}

#[component]
fn LegacyDividendTables(data: LegacyDividendData) -> Element {
    rsx! {
        div { class: "stack",
            LegacySummaryTable { rows: data.summaries }
            LegacyMonthlyTable { rows: data.monthly }
        }
    }
}

#[component]
fn LegacySummaryTable(rows: Vec<LegacyDividendSummaryRow>) -> Element {
    rsx! {
        section { class: "card table-card",
            div { class: "table-summary",
                strong { "年度／累積資料" }
                span { "{rows.len()} 筆，來自 dividend_legacy_summary" }
            }
            div { class: "table-wrap",
                table { class: "legacy-summary-table",
                    thead {
                        tr {
                            th { "所有權人" }
                            th { "代號" }
                            th { "商品" }
                            th { "期間類型" }
                            th { "原始標籤" }
                            th { "金額" }
                            th { "Excel 儲存格" }
                        }
                    }
                    tbody {
                        for row in rows {
                            LegacySummaryRowView { row }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn LegacySummaryRowView(row: LegacyDividendSummaryRow) -> Element {
    rsx! {
        tr {
            td { "{row.owner_name}" }
            td { class: "mono", "{row.symbol}" }
            td { class: "name-cell", "{row.instrument_name}" }
            td { "{period_label_text(&row.period_label)}" }
            td { class: "mono", "{row.period_label}" }
            td { class: "number strong", "{money(row.amount)}" }
            td { class: "mono", "{row.source_cell}" }
        }
    }
}

#[component]
fn LegacyMonthlyTable(rows: Vec<LegacyDividendMonthlyRow>) -> Element {
    rsx! {
        section { class: "card table-card",
            div { class: "table-summary",
                strong { "月份資料" }
                span { "{rows.len()} 筆，來自 dividend_legacy_monthly" }
            }
            div { class: "table-wrap",
                table { class: "legacy-monthly-table",
                    thead {
                        tr {
                            th { "所有權人" }
                            th { "代號" }
                            th { "商品" }
                            th { "資料類型" }
                            th { "原始標籤" }
                            th { "月份" }
                            th { "金額" }
                            th { "Excel 儲存格" }
                        }
                    }
                    tbody {
                        for row in rows {
                            LegacyMonthlyRowView { row }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn LegacyMonthlyRowView(row: LegacyDividendMonthlyRow) -> Element {
    rsx! {
        tr {
            td { "{row.owner_name}" }
            td { class: "mono", "{row.symbol}" }
            td { class: "name-cell", "{row.instrument_name}" }
            td { "{series_type_text(&row.series_type)}" }
            td { class: "mono", "{row.series_type}" }
            td { class: "number", "{row.month_num} 月" }
            td { class: "number strong", "{money(row.amount)}" }
            td { class: "mono", "{row.source_cell}" }
        }
    }
}

fn period_label_text(label: &str) -> &'static str {
    match label {
        "YEAR_2023" => "2023 年股息總額",
        "YEAR_2024" => "2024 年股息總額",
        "THROUGH_PREVIOUS_YEAR" => "截至上一年度累積",
        "TOTAL_CUMULATIVE" => "總累積",
        "CURRENT_YEAR_TO_DATE" => "今年度累積",
        _ => "其他",
    }
}

fn series_type_text(series_type: &str) -> &'static str {
    match series_type {
        "ACTUAL_CURRENT_YEAR" => "當年度實際月份股息",
        "FORECAST_AVERAGE" => "預估／平均月份配息",
        _ => "其他",
    }
}

#[component]
fn HoldingsTable(rows: Vec<HoldingMetric>) -> Element {
    rsx! {
        section { class: "card table-card",
            div { class: "table-summary",
                strong { "{rows.len()} 筆持股" }
                span { "依目前市值由高到低排序" }
            }
            div { class: "table-wrap",
                table { class: "holdings-table",
                    thead {
                        tr {
                            th { "所有權人" }
                            th { "證券帳戶" }
                            th { "代號" }
                            th { "商品名稱" }
                            th { "類型" }
                            th { "資產類別" }
                            th { "區域" }
                            th { "數量" }
                            th { "平均成本" }
                            th { "市價" }
                            th { "總成本" }
                            th { "市值" }
                            th { "未實現損益" }
                            th { "損益率" }
                            th { "預估年配息" }
                            th { "預估殖利率" }
                            th { "更新日" }
                        }
                    }
                    tbody {
                        for row in rows {
                            HoldingRow { row }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn AccountAssetsTable(rows: Vec<AccountAsset>) -> Element {
    rsx! {
        section { class: "card table-card",
            div { class: "table-summary",
                strong { "{rows.len()} 筆帳戶資產" }
                span { "依台幣價值由高到低排序" }
            }
            div { class: "table-wrap",
                table { class: "account-assets-table",
                    thead {
                        tr {
                            th { "所有權人" }
                            th { "金融機構" }
                            th { "帳戶名稱" }
                            th { "帳戶類型" }
                            th { "資產類型" }
                            th { "幣別" }
                            th { "原幣金額" }
                            th { "台幣價值" }
                            th { "更新日" }
                        }
                    }
                    tbody {
                        for row in rows {
                            AccountAssetRow { row }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn AccountAssetRow(row: AccountAsset) -> Element {
    rsx! {
        tr {
            td { "{row.owner_name}" }
            td { "{row.institution_name}" }
            td { class: "name-cell", "{row.account_name}" }
            td { "{row.account_type}" }
            td { "{row.asset_type}" }
            td { class: "mono", "{row.currency_code}" }
            td { class: "number", "{decimal(row.original_amount, 2)}" }
            td { class: "number strong", "{money(row.current_value_ntd)}" }
            td { class: "mono", "{row.snapshot_date}" }
        }
    }
}

#[component]
fn HoldingRow(row: HoldingMetric) -> Element {
    let profit_class = match row.unrealized_profit {
        Some(value) if value > 0.0 => "number positive",
        Some(value) if value < 0.0 => "number negative",
        _ => "number muted",
    };

    rsx! {
        tr {
            td { "{row.owner_name}" }
            td { "{row.account_name}" }
            td { class: "mono", "{row.symbol}" }
            td { class: "name-cell", "{row.instrument_name}" }
            td { "{row.instrument_type}" }
            td { "{row.asset_class}" }
            td { "{row.region_type}" }
            td { class: "number", "{decimal(row.quantity, 2)}" }
            td { class: "number", "{decimal(row.average_cost, 2)}" }
            td { class: "number", "{decimal(row.market_price, 2)}" }
            td { class: "number", "{money(row.total_cost)}" }
            td { class: "number strong", "{money(row.market_value)}" }
            td { class: profit_class, "{money(row.unrealized_profit)}" }
            td { class: profit_class, "{percent(row.unrealized_return_rate)}" }
            td { class: "number", "{money(row.estimated_annual_dividend)}" }
            td { class: "number", "{percent(row.estimated_yield_on_cost)}" }
            td { class: "mono", "{row.snapshot_date}" }
        }
    }
}
