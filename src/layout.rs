use dioxus::prelude::*;

use crate::routes::Route;

#[component]
pub fn AppLayout() -> Element {
    rsx! {
        div { class: "app-shell",
            aside { class: "sidebar",
                div { class: "brand",
                    span { class: "brand-mark", "BOM" }
                    div {
                        h1 { "資產管理" }
                        p { "Personal Balance Office" }
                    }
                }
                nav { class: "nav-list",
                    NavLink { route: Route::DashboardPage {}, label: "總覽" }
                    NavLink { route: Route::AccountsPage {}, label: "帳戶資產" }
                    NavLink { route: Route::HoldingsPage {}, label: "持股明細" }
                    NavLink { route: Route::DividendIncomePage {}, label: "股息收入" }
                    NavLink { route: Route::DividendsLegacyPage {}, label: "Excel 歷史股息" }
                }
            }
            main { class: "content",
                Outlet::<Route> {}
            }
        }
    }
}

#[component]
fn NavLink(route: Route, label: &'static str) -> Element {
    rsx! {
        Link { class: "nav-link", active_class: "active", to: route, "{label}" }
    }
}
