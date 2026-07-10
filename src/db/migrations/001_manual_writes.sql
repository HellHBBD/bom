CREATE UNIQUE INDEX IF NOT EXISTS uq_manual_asset_snapshot
ON account_asset_snapshot (
    account_id,
    snapshot_date,
    asset_type,
    currency_code
)
WHERE origin = 'MANUAL';

CREATE UNIQUE INDEX IF NOT EXISTS uq_manual_holding_snapshot
ON holding_snapshot (
    account_id,
    instrument_id,
    snapshot_date
)
WHERE origin = 'MANUAL';

CREATE UNIQUE INDEX IF NOT EXISTS uq_manual_instrument_price_account
ON instrument_price (
    account_id,
    instrument_id,
    price_date
)
WHERE origin = 'MANUAL' AND account_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_manual_instrument_price_global
ON instrument_price (
    instrument_id,
    price_date
)
WHERE origin = 'MANUAL' AND account_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_manual_dividend_assumption
ON dividend_assumption (
    account_id,
    instrument_id,
    effective_date
)
WHERE origin = 'MANUAL';

-- dividend_receipt intentionally has no business unique index in Stage 4.
-- Same-day same-instrument receipts can be legitimate; future duplicate
-- prevention should use a deliberate external reference or fingerprint.
