DROP VIEW IF EXISTS v_asset_total;
DROP VIEW IF EXISTS v_holding_metrics;
DROP VIEW IF EXISTS v_latest_dividend_assumption;
DROP INDEX IF EXISTS uq_manual_dividend_assumption;
DROP INDEX IF EXISTS idx_dividend_assumption_account_date;
DROP INDEX IF EXISTS idx_dividend_assumption_instrument_date;

CREATE TABLE dividend_assumption_new (
    assumption_id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES account(account_id),
    instrument_id INTEGER NOT NULL REFERENCES instrument(instrument_id),
    effective_date TEXT NOT NULL,
    payments_per_year INTEGER CHECK (payments_per_year >= 0),
    latest_dividend_per_unit_text TEXT,
    estimated_annual_dividend_per_unit_text TEXT,
    currency_code TEXT NOT NULL REFERENCES currency(currency_code),
    note TEXT,
    source_sheet TEXT,
    source_row INTEGER,
    origin TEXT NOT NULL DEFAULT 'EXCEL_IMPORT'
        CHECK (origin IN ('EXCEL_IMPORT', 'MANUAL')),
    UNIQUE (account_id, instrument_id, effective_date, source_row)
);

INSERT INTO dividend_assumption_new (
    account_id,
    instrument_id,
    effective_date,
    payments_per_year,
    latest_dividend_per_unit_text,
    estimated_annual_dividend_per_unit_text,
    currency_code,
    note,
    source_sheet,
    source_row,
    origin
)
SELECT
    account_id,
    instrument_id,
    effective_date,
    payments_per_year,
    latest_dividend_per_unit_text,
    estimated_annual_dividend_per_unit_text,
    currency_code,
    note,
    source_sheet,
    source_row,
    origin
FROM (
    SELECT
        source_rows.*,
        ROW_NUMBER() OVER (
            PARTITION BY source_rows.account_id,
                source_rows.instrument_id,
                source_rows.effective_date,
                CASE
                    WHEN source_rows.origin = 'MANUAL' THEN -1
                    ELSE COALESCE(source_rows.source_row, -1)
                END
            ORDER BY CASE WHEN source_rows.origin = 'MANUAL' THEN 0 ELSE 1 END,
                source_rows.assumption_id DESC
        ) AS row_rank
    FROM (
        SELECT
            da.assumption_id,
            da.account_id,
            da.instrument_id,
            da.effective_date,
            da.payments_per_year,
            da.latest_dividend_per_unit_text,
            da.estimated_annual_dividend_per_unit_text,
            da.currency_code,
            da.note,
            da.source_sheet,
            da.source_row,
            da.origin
        FROM dividend_assumption_account_archive da

        UNION ALL

        SELECT
            da.assumption_id,
            hs.account_id,
            da.instrument_id,
            da.effective_date,
            da.payments_per_year,
            da.latest_dividend_per_unit_text,
            da.estimated_annual_dividend_per_unit_text,
            da.currency_code,
            da.note,
            da.source_sheet,
            da.source_row,
            da.origin
        FROM dividend_assumption da
        JOIN (
            SELECT DISTINCT account_id, instrument_id
            FROM holding_snapshot
        ) hs ON hs.instrument_id = da.instrument_id
    ) AS source_rows
)
WHERE row_rank = 1;

DROP TABLE dividend_assumption;
ALTER TABLE dividend_assumption_new RENAME TO dividend_assumption;

CREATE INDEX idx_dividend_assumption_account_date
ON dividend_assumption(account_id, instrument_id, effective_date);

CREATE INDEX idx_dividend_assumption_instrument_date
ON dividend_assumption(instrument_id, effective_date);

CREATE UNIQUE INDEX uq_manual_dividend_assumption
ON dividend_assumption (
    account_id,
    instrument_id,
    effective_date
)
WHERE origin = 'MANUAL';

CREATE VIEW v_latest_dividend_assumption AS
SELECT *
FROM (
    SELECT
        da.*,
        ROW_NUMBER() OVER (
            PARTITION BY da.account_id, da.instrument_id
            ORDER BY da.effective_date DESC,
                CASE WHEN da.origin = 'MANUAL' THEN 0 ELSE 1 END,
                da.assumption_id DESC
        ) AS row_rank
    FROM dividend_assumption da
)
WHERE row_rank = 1;

CREATE VIEW v_holding_metrics AS
SELECT
    h.holding_snapshot_id,
    h.account_id,
    ao.person_id,
    p.display_name AS owner_name,
    h.instrument_id,
    i.symbol,
    i.name AS instrument_name,
    i.instrument_type,
    i.asset_class,
    i.region_type,
    i.trading_currency_code,
    h.snapshot_date,
    CAST(h.quantity_text AS REAL) AS quantity,
    CAST(h.average_cost_text AS REAL) AS average_cost,
    pr.price_date AS market_price_date,
    pr.currency_code AS market_price_currency_code,
    CAST(pr.price_text AS REAL) AS market_price,
    CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) AS total_cost,
    CAST(h.quantity_text AS REAL) * CAST(pr.price_text AS REAL) AS market_value,
    CAST(h.quantity_text AS REAL)
        * (CAST(pr.price_text AS REAL) - CAST(h.average_cost_text AS REAL))
        AS unrealized_profit,
    CASE
        WHEN CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) = 0 THEN NULL
        ELSE (
            CAST(h.quantity_text AS REAL)
            * (CAST(pr.price_text AS REAL) - CAST(h.average_cost_text AS REAL))
        ) / (
            CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL)
        )
    END AS unrealized_return_rate,
    da.effective_date AS dividend_effective_date,
    da.currency_code AS dividend_currency_code,
    CAST(da.estimated_annual_dividend_per_unit_text AS REAL)
        AS estimated_annual_dividend_per_unit,
    CAST(h.quantity_text AS REAL)
        * CAST(da.estimated_annual_dividend_per_unit_text AS REAL)
        AS estimated_annual_dividend,
    CASE
        WHEN CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) = 0 THEN NULL
        ELSE (
            CAST(h.quantity_text AS REAL)
            * CAST(da.estimated_annual_dividend_per_unit_text AS REAL)
        ) / (
            CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL)
        )
    END AS estimated_yield_on_cost,
    da.payments_per_year,
    CAST(da.latest_dividend_per_unit_text AS REAL) AS latest_dividend_per_unit
FROM v_latest_holding h
JOIN instrument i ON i.instrument_id = h.instrument_id
LEFT JOIN v_latest_instrument_price pr
       ON pr.instrument_id = h.instrument_id
LEFT JOIN v_latest_dividend_assumption da
       ON da.account_id = h.account_id
      AND da.instrument_id = h.instrument_id
LEFT JOIN account_owner ao ON ao.account_id = h.account_id
LEFT JOIN person p ON p.person_id = ao.person_id;

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
