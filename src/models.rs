#[derive(Clone, Debug, PartialEq)]
pub struct HoldingMetric {
    pub holding_snapshot_id: i64,
    pub account_id: i64,
    pub instrument_id: i64,
    pub owner_name: String,
    pub account_name: String,
    pub account_number: Option<String>,
    pub symbol: String,
    pub instrument_name: String,
    pub instrument_type: String,
    pub asset_class: String,
    pub region_type: String,
    pub trading_currency_code: String,
    pub cost_currency_code: String,
    pub snapshot_date: String,
    pub quantity: Option<f64>,
    pub average_cost: Option<f64>,
    pub average_cost_text: String,
    pub buy_fee_rate: Option<f64>,
    pub applied_buy_fee_rate_text: String,
    pub sell_fee_rate: Option<f64>,
    pub sell_transaction_tax_rate: Option<f64>,
    pub note: String,
    pub market_price_date: Option<String>,
    pub market_price_currency_code: Option<String>,
    pub market_price: Option<f64>,
    pub total_cost: Option<f64>,
    pub market_value: Option<f64>,
    pub liquidation_value: Option<f64>,
    pub unrealized_profit: Option<f64>,
    pub unrealized_return_rate: Option<f64>,
    pub dividend_effective_date: Option<String>,
    pub dividend_currency_code: Option<String>,
    pub estimated_annual_dividend_per_unit: Option<f64>,
    pub payments_per_year: Option<i64>,
    pub latest_dividend_per_unit: Option<f64>,
    pub estimated_annual_dividend: Option<f64>,
    pub estimated_yield_on_cost: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AccountAsset {
    pub snapshot_id: i64,
    pub account_id: i64,
    pub origin: String,
    pub owner_name: String,
    pub institution_name: String,
    pub account_name: String,
    pub account_number: Option<String>,
    pub account_type: String,
    pub asset_type: String,
    pub currency_code: String,
    pub quantity_text: Option<String>,
    pub invested_amount_text: Option<String>,
    pub current_value_override_text: Option<String>,
    pub note: String,
    pub quantity: Option<f64>,
    pub invested_amount: Option<f64>,
    pub current_value_ntd: Option<f64>,
    pub snapshot_date: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExchangeRatePreview {
    pub rate_text: String,
    pub rate_date: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExchangeRateRow {
    pub exchange_rate_id: i64,
    pub rate_date: String,
    pub base_currency_code: String,
    pub quote_currency_code: String,
    pub rate_text: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AnnualDividendRow {
    pub dividend_year: i64,
    pub annual_dividend_per_unit_text: String,
    pub currency_code: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DashboardSummary {
    pub total_assets: Option<f64>,
    pub account_assets: Option<f64>,
    pub investment_assets: Option<f64>,
    pub account_asset_count: i64,
    pub holding_count: i64,
    pub account_asset_missing_value_count: i64,
    pub holding_missing_market_value_count: i64,
    pub holding_missing_dividend_count: i64,
    pub estimated_annual_dividend: Option<f64>,
    pub estimated_monthly_dividend: Option<f64>,
    pub latest_account_asset_date: Option<String>,
    pub latest_holding_date: Option<String>,
    pub owner_totals: Vec<OwnerAssetTotal>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OwnerAssetTotal {
    pub owner_name: String,
    pub value_ntd: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DividendReceiptRow {
    pub receipt_id: i64,
    pub account_id: i64,
    pub instrument_id: i64,
    pub origin: String,
    pub owner_name: String,
    pub account_name: String,
    pub account_number: Option<String>,
    pub symbol: String,
    pub instrument_name: String,
    pub received_on: String,
    pub gross_amount: Option<f64>,
    pub tax_amount: Option<f64>,
    pub fee_amount: Option<f64>,
    pub net_amount: Option<f64>,
    pub currency_code: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DividendReceiptAccountOption {
    pub account_id: i64,
    pub owner_name: String,
    pub account_name: String,
    pub account_number: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DividendReceiptInstrumentOption {
    pub instrument_id: i64,
    pub symbol: String,
    pub instrument_name: String,
    pub currency_code: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DividendReceiptFormOptions {
    pub accounts: Vec<DividendReceiptAccountOption>,
    pub instruments: Vec<DividendReceiptInstrumentOption>,
    pub currency_codes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegacyDividendSummaryRow {
    pub owner_name: String,
    pub symbol: String,
    pub instrument_name: String,
    pub period_label: String,
    pub amount: Option<f64>,
    pub source_cell: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegacyDividendMonthlyRow {
    pub owner_name: String,
    pub symbol: String,
    pub instrument_name: String,
    pub series_type: String,
    pub month_num: i64,
    pub amount: Option<f64>,
    pub source_cell: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegacyDividendData {
    pub summaries: Vec<LegacyDividendSummaryRow>,
    pub monthly: Vec<LegacyDividendMonthlyRow>,
}
