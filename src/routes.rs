use dioxus::prelude::*;

use crate::pages::{
    AccountsPage, DashboardPage, DividendIncomePage, DividendsLegacyPage, HoldingsPage,
};

#[derive(Clone, Routable, PartialEq)]
pub enum Route {
    #[layout(crate::layout::AppLayout)]
    #[route("/")]
    DashboardPage {},
    #[route("/accounts")]
    AccountsPage {},
    #[route("/holdings")]
    HoldingsPage {},
    #[route("/dividends")]
    DividendIncomePage {},
    #[route("/dividends/legacy")]
    DividendsLegacyPage {},
}
