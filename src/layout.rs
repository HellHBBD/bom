use dioxus::prelude::*;

use crate::routes::Route;
use crate::ui_preference::{
    load_all_preferences, persist_preference, preference_value, LAST_ROUTE,
};

#[derive(Clone, Debug, PartialEq)]
enum StartupState {
    Loading,
    RestoringRoute(String),
    Ready,
}

#[component]
pub fn AppLayout() -> Element {
    let mut preferences = use_signal(std::collections::HashMap::new);
    use_context_provider(|| preferences);
    let mut startup = use_signal(|| StartupState::Loading);
    let preferences_result = use_resource(|| async move {
        #[cfg(not(target_arch = "wasm32"))]
        {
            tokio::task::spawn_blocking(load_all_preferences)
                .await
                .map_err(|error| format!("UI 偏好設定載入工作失敗：{error}"))?
        }
        #[cfg(target_arch = "wasm32")]
        {
            load_all_preferences()
        }
    });
    let navigator = use_navigator();
    let router = router();
    let current_path = router.full_route_string();

    use_effect(move || {
        if startup() != StartupState::Loading {
            return;
        }
        if let Some(Ok(values)) = preferences_result() {
            let route = preference_value(&values, LAST_ROUTE);
            preferences.set(values);
            if let Some(route) = saved_route_to_restore(&route, &current_path) {
                startup.set(StartupState::RestoringRoute(route_path(&route).to_string()));
                navigator.replace(route);
            } else {
                startup.set(StartupState::Ready);
            }
        } else if let Some(Err(error)) = preferences_result() {
            eprintln!("讀取 UI 偏好設定失敗，將使用預設值：{error}");
            startup.set(StartupState::Ready);
        }
    });

    use_effect(move || {
        if let StartupState::RestoringRoute(restored_path) = startup() {
            if restored_path == router.full_route_string() {
                startup.set(StartupState::Ready);
            }
        }
    });

    use_effect(move || {
        let current_path = router.full_route_string();
        if startup() == StartupState::Ready {
            persist_preference(preferences, LAST_ROUTE, current_path);
        }
    });

    if startup() != StartupState::Ready {
        return match preferences_result() {
            None => rsx! { main { class: "content", "載入 UI 偏好設定中..." } },
            Some(Err(_)) => rsx! { main { class: "content", "套用預設 UI 設定中..." } },
            Some(Ok(_)) => rsx! { main { class: "content", "套用 UI 偏好設定中..." } },
        };
    }

    rsx! {
        div { class: "app-shell",
            aside { class: "sidebar",
                div { class: "brand",
                    img {
                        class: "brand-mark",
                        src: asset!("/assets/icon.svg"),
                        alt: "BOM 圖示",
                    }
                    div {
                        h1 { "資產管理" }
                    }
                }
                nav { class: "nav-list",
                    NavLink { route: Route::DashboardPage {}, label: "總覽" }
                    NavLink { route: Route::AccountsPage {}, label: "帳戶資產" }
                    NavLink { route: Route::HoldingsPage {}, label: "持股明細" }
                    NavLink { route: Route::QuickPriceUpdatePage {}, label: "快速市價更新" }
                    NavLink { route: Route::ExchangeRatePage {}, label: "匯率維護" }
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
        Link {
            class: "nav-link",
            active_class: "active",
            to: route,
            "{label}"
        }
    }
}

fn route_from_preference(path: &str) -> Option<Route> {
    match path {
        "/" => Some(Route::DashboardPage {}),
        "/accounts" => Some(Route::AccountsPage {}),
        "/holdings" => Some(Route::HoldingsPage {}),
        "/market/prices" => Some(Route::QuickPriceUpdatePage {}),
        "/market/exchange-rates" => Some(Route::ExchangeRatePage {}),
        "/dividends" => Some(Route::DividendIncomePage {}),
        "/dividends/legacy" => Some(Route::DividendsLegacyPage {}),
        _ => None,
    }
}

fn saved_route_to_restore(saved_path: &str, current_path: &str) -> Option<Route> {
    route_from_preference(saved_path).filter(|route| route_path(route) != current_path)
}

fn route_path(route: &Route) -> &'static str {
    match route {
        Route::DashboardPage {} => "/",
        Route::AccountsPage {} => "/accounts",
        Route::HoldingsPage {} => "/holdings",
        Route::QuickPriceUpdatePage {} => "/market/prices",
        Route::ExchangeRatePage {} => "/market/exchange-rates",
        Route::DividendIncomePage {} => "/dividends",
        Route::DividendsLegacyPage {} => "/dividends/legacy",
    }
}

#[cfg(test)]
mod tests {
    use super::{route_from_preference, saved_route_to_restore};

    #[test]
    fn restores_only_defined_routes() {
        assert!(route_from_preference("/holdings").is_some());
        assert!(route_from_preference("/not-a-route").is_none());
    }

    #[test]
    fn restores_a_saved_route_only_when_it_differs_from_the_current_route() {
        assert_eq!(
            saved_route_to_restore("/holdings", "/")
                .as_ref()
                .map(super::route_path),
            Some("/holdings")
        );
        assert!(saved_route_to_restore("/holdings", "/holdings").is_none());
        assert!(saved_route_to_restore("/missing", "/").is_none());
    }
}
