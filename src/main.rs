use dioxus::prelude::*;

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    rsx! {
        main {
            style: "
                min-height: 100vh;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                font-family: sans-serif;
            ",

            h1 { "Asset Manager" }
            p { "Dioxus Desktop is running." }
        }
    }
}
