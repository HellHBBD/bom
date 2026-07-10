DROP VIEW IF EXISTS v_asset_total;
DROP VIEW IF EXISTS v_account_asset_value;

ALTER TABLE exchange_rate RENAME TO exchange_rate_legacy;

CREATE TABLE exchange_rate (
    exchange_rate_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rate_date TEXT NOT NULL,
    base_currency_code TEXT NOT NULL REFERENCES currency(currency_code),
    quote_currency_code TEXT NOT NULL REFERENCES currency(currency_code),
    rate_text TEXT NOT NULL,
    origin TEXT NOT NULL DEFAULT 'EXCEL_IMPORT'
        CHECK (origin IN ('EXCEL_IMPORT', 'MANUAL')),
    note TEXT,
    source_sheet TEXT,
    source_cell TEXT,
    CHECK (base_currency_code <> quote_currency_code)
);

INSERT INTO exchange_rate (
    rate_date,
    base_currency_code,
    quote_currency_code,
    rate_text,
    origin,
    note,
    source_sheet,
    source_cell
)
SELECT
    rate_date,
    base_currency_code,
    quote_currency_code,
    rate_text,
    'EXCEL_IMPORT',
    NULL,
    source_sheet,
    source_cell
FROM exchange_rate_legacy;

DROP TABLE exchange_rate_legacy;

CREATE UNIQUE INDEX uq_exchange_rate_origin
ON exchange_rate (
    rate_date,
    base_currency_code,
    quote_currency_code,
    origin
);

CREATE UNIQUE INDEX uq_manual_exchange_rate
ON exchange_rate (
    rate_date,
    base_currency_code,
    quote_currency_code
)
WHERE origin = 'MANUAL';

CREATE INDEX idx_exchange_rate_lookup
ON exchange_rate (
    base_currency_code,
    quote_currency_code,
    rate_date
);

CREATE VIEW v_account_asset_value AS
SELECT
    s.snapshot_id,
    s.account_id,
    s.snapshot_date,
    s.asset_type,
    s.currency_code,
    s.invested_amount_text,
    s.quantity_text,
    s.current_value_override_text,
    COALESCE(
        CAST(s.current_value_override_text AS REAL),
        CASE
            WHEN s.quantity_text IS NOT NULL AND s.currency_code = 'NTD'
                THEN CAST(s.quantity_text AS REAL)
            WHEN s.quantity_text IS NOT NULL AND xr.rate_text IS NOT NULL
                THEN CAST(s.quantity_text AS REAL) * CAST(xr.rate_text AS REAL)
            ELSE CAST(s.invested_amount_text AS REAL)
        END
    ) AS current_value_ntd
FROM v_latest_account_asset_snapshot s
LEFT JOIN exchange_rate xr
    ON xr.exchange_rate_id = (
        SELECT ex.exchange_rate_id
        FROM exchange_rate ex
        WHERE ex.base_currency_code = s.currency_code
          AND ex.quote_currency_code = 'NTD'
          AND ex.rate_date <= s.snapshot_date
        ORDER BY ex.rate_date DESC,
                 CASE ex.origin WHEN 'MANUAL' THEN 0 ELSE 1 END,
                 ex.exchange_rate_id DESC
        LIMIT 1
    );

CREATE VIEW v_asset_total AS
SELECT
    p.person_id,
    p.display_name AS owner_name,
    'ACCOUNT_ASSET' AS source_type,
    av.account_id,
    NULL AS instrument_id,
    av.current_value_ntd AS value_ntd
FROM v_account_asset_value av
JOIN account_owner ao ON ao.account_id = av.account_id
JOIN person p ON p.person_id = ao.person_id

UNION ALL

SELECT
    hm.person_id,
    hm.owner_name,
    'HOLDING' AS source_type,
    hm.account_id,
    hm.instrument_id,
    hm.market_value AS value_ntd
FROM v_holding_metrics hm;
