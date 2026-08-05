# UX Improvement Plan

## Purpose

Improve the asset-management workflow based on the desktop UI review. The work
prioritizes task completion, input efficiency, clear data meaning, keyboard
operation, and recovery from errors over visual restyling.

## Current Status

Completed but not yet committed in the current worktree:

- Filters have accessible names and use the dark native select color scheme.
- The holding table action column remains visible while horizontally scrolling.
- The dividend settings actions appear before the annual-dividend section.
- The dividend receipt modal shows save-and-continue feedback inside the modal.
- Exchange-rate deletion uses a foreground confirmation modal.
- Dashboard missing-data cards open a corresponding filtered holdings list.
- User-facing descriptions no longer expose internal SQL source names.
- Quick-price controls remain visible while scrolling, show entered-row counts, and use decimal inputs with an explicit price placeholder.
- Reset actions are in every modal footer, are labelled `重設欄位`, and are disabled until the form changes.
- The dividend receipt empty state distinguishes actual receipts from Excel history.
- Dividend receipt save-and-continue returns focus to the product control.

Previously committed:

- Annual dividend history, manual fallback behavior, and validation.
- Modal focus trapping and focus restoration.
- Holding update defaults, clearer labels, and keyboard shortcuts.
- Runtime SQLite baseline schema validation.

## Review Findings

### Completed Findings

1. Filter controls had unreadable native select rendering and incomplete
   accessible names.
2. Holding row actions were outside the default visible table width.
3. The dividend settings save action followed the annual-dividend section.
4. Save-and-continue feedback was hidden by the modal backdrop.
5. Exchange-rate deletion confirmation appeared below the rate table.

### Remaining Findings

No functional findings remain. Visual review at the target desktop sizes is
still required before release.

## Implementation Plan

### 1. Guide Users To Missing Data

Files:

- `src/routes.rs`
- `src/pages.rs`
- `src/ui_preference.rs`

Add issue-specific holdings navigation for missing dividends and missing market
values.

1. Extend the holdings route with an optional issue parameter.
2. Recognize `missing_dividend` and `missing_market_value` in the holdings
   page.
3. Apply the issue filter without overwriting the user's saved regular filter
   preferences.
4. Add a visible issue-filter summary and a clear action.
5. Make dashboard missing-data counts link to the corresponding filtered
   holdings view.
6. Keep the holding row actions available so users can immediately update the
   holding or manage its dividend assumptions.

Verification:

- A dashboard missing-dividend count opens only holdings without an annual
  dividend estimate.
- A missing-market-value count opens only holdings without a market value.
- Clearing the issue filter restores the normal holdings list.

### 2. Replace Internal Data Names

File:

- `src/pages.rs`

Replace technical SQL references with task-focused descriptions.

| Current wording | Replacement |
| --- | --- |
| `v_account_asset_value` | Latest account assets and NTD-converted values |
| `v_holding_metrics` | Latest holdings, costs, values, gains, and dividend estimates |
| `dividend_receipt / v_dividend_receipt_amount` | Actual received dividend records |
| `dividend_legacy_summary` | Excel-imported historical dividend summaries |

Technical names remain appropriate for developer documentation, not normal UI.

Verification:

- Main pages, table summaries, and empty states contain no SQL table or view
  names.

### 3. Keep Quick Price Actions Reachable

Files:

- `src/pages.rs`
- `assets/main.css`

1. Reserve Ctrl/Cmd+Enter for saving entered prices; ordinary Enter remains an
   input-navigation action.

Verification:

- Price entries near the end of a long list can be saved without returning to
  the top.
- Only entered rows are saved.
- Clearing inputs does not modify existing prices.
- The action bar remains usable at the minimum desktop window size.

### 4. Standardize Modal Reset Actions

Files:

- `src/pages.rs`
- `assets/main.css`

1. Standardize footer order:
   - Secondary: reset
   - Right-aligned: cancel, primary save
2. Keep confirmation and destructive actions visually distinct.

Verification:

- No reset control is mistaken for a close control.
- Tab order follows fields, reset, cancel, and save.
- Save-and-continue allows immediate selection of the next product.

### 5. Clarify Dividend Receipt Empty State

File:

- `src/pages.rs`

Replace conflicting wording with:

- Title: `目前沒有實際入帳股息`
- Description: `可點「新增股息」記錄實際入帳資料；Excel 歷史彙總保留在「Excel 歷史股息」供查閱。`

Keep the link to the historical Excel summary.

Verification:

- The empty state clearly distinguishes editable actual receipts from read-only
  historical summaries.

## Commit Plan

1. `feat: guide users to incomplete asset data`
2. `refactor: simplify financial data labels`
3. `feat: keep quick price actions accessible`
4. `refactor: standardize modal reset actions`
5. `fix: clarify dividend empty state`

## Validation

Run after each independently shippable change:

```bash
cargo fmt --check
git diff --check
cargo test --locked
cargo check --locked
```

## Visual Verification

Capture and review the following at `1440x900` and `1024x700`:

1. Dashboard with a missing-dividend or missing-market-value count.
2. Holdings filtered to a missing-data issue.
3. Quick price update page near the final table rows.
4. Dividend receipt empty state.
5. Dividend receipt save-and-continue state with product focus restored.
6. A representative modal with the reset action in its footer.
