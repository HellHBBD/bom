# AGENTS.md

## Project Overview

This project is a Dioxus 0.7 Desktop application (Rust) targeting:

- Linux
- Windows

The application provides:

- Native desktop UI
- CSV file import (via native file dialog)
- Local SQLite storage
- Paginated table display
- No server and no fullstack backend

This project is desktop-only.

---

## Development Commands

### Run in Desktop mode

```bash
dx serve --platform desktop
```

### Build Release Binary

```bash
cargo build --release
```

Binary location:

```text
target/release/<app_name>
```

### Lint and Format

- Format: `cargo fmt`
- Lint: `cargo clippy`

---

## Architecture Overview

### UI Layer

- Built with Dioxus 0.7
- Uses signals (`use_signal`)
- Uses `spawn` plus `tokio::task::spawn_blocking` for blocking operations

### Data Layer (Local Only)

- SQLite via `rusqlite`
- CSV parsing via `csv`
- Database stored in OS-specific app data directory
- No remote database
- No API calls
- No server functions

---

## Dependencies

```toml
dioxus = { version = "0.7.1", features = ["desktop"] }

rfd = "0.14"
csv = "1.3"
rusqlite = { version = "0.31", features = ["bundled"] }
directories = "5"
anyhow = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Important:

- `rusqlite` uses `bundled` feature for cross-platform builds.
- No Postgres/MySQL.
- No Axum.
- No server feature enabled.

---

## Database Design

This project uses an EAV schema for maximum CSV flexibility.

### Tables

#### dataset

Stores import metadata.

#### column_name

Stores column headers.

#### cell

Stores individual CSV cell values.

Structure:

```text
dataset(id)
column_name(dataset_id, col_idx, name)
cell(dataset_id, row_idx, col_idx, value)
```

Multiple datasets are kept and can be switched in the UI.

---

## CSV Import Flow

1. User clicks "Import CSV"
2. Native file dialog opens (`rfd`)
3. CSV parsed
4. Data inserted inside SQLite transaction
5. UI refreshes to first page

---

## Pagination

- Page size constant: 50 rows
- SQL range query using `row_idx`
- UI Prev/Next controls

---

## Threading Model

All blocking operations are executed inside:

```rust
tokio::task::spawn_blocking(...)
```

Never block the UI thread.

---

## Cross-Platform Notes

### Windows

- No external SQLite required
- Uses bundled SQLite

### Linux

- No system SQLite required
- Uses bundled SQLite

### File Paths

Always use:

```rust
directories::ProjectDirs
```

Never hardcode:

- `/home/...`
- `C:\...`

---

## Project Structure

```text
src/
 ├── main.rs
 ├── desktop_db.rs
 └── components/
```

`desktop_db.rs` handles:

- `init_db`
- `import_csv_to_sqlite`
- `query_page`

UI should not contain SQL logic.

---

## Design Principles

- Desktop-only application
- No backend server
- No web target
- Local-first architecture
- SQLite as embedded storage
- UI responsive at all times
- Large CSV files supported (via pagination)

---

## Do Not Add

Do NOT introduce:

- fullstack feature
- server functions
- remote DB connection
- web platform
- Postgres/MySQL drivers

This is strictly a desktop-native application.

---

## Future Extensions (Optional)

- Column sorting
- Search/filter (SQL WHERE)
- Export CSV
- Editable table cells
