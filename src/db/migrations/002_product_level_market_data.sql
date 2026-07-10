DROP VIEW IF EXISTS v_asset_total;
DROP VIEW IF EXISTS v_holding_metrics;
DROP VIEW IF EXISTS v_latest_holding;
DROP VIEW IF EXISTS v_latest_instrument_price;
DROP VIEW IF EXISTS v_latest_dividend_assumption;
DROP INDEX IF EXISTS uq_manual_dividend_assumption;

ALTER TABLE instrument_price RENAME TO instrument_price_account_archive;

CREATE TABLE instrument_price (
    price_id INTEGER PRIMARY KEY,
    instrument_id INTEGER NOT NULL REFERENCES instrument(instrument_id),
    price_date TEXT NOT NULL,
    price_text TEXT NOT NULL,
    currency_code TEXT NOT NULL REFERENCES currency(currency_code),
    source TEXT,
    source_cell TEXT,
    origin TEXT NOT NULL DEFAULT 'EXCEL_IMPORT'
        CHECK (origin IN ('EXCEL_IMPORT', 'MANUAL')),
    UNIQUE (instrument_id, price_date, origin)
);

INSERT INTO instrument_price (
    price_id,
    instrument_id,
    price_date,
    price_text,
    currency_code,
    source,
    source_cell,
    origin
)
SELECT
    price_id,
    instrument_id,
    price_date,
    price_text,
    currency_code,
    source,
    source_cell,
    origin
    FROM (
        SELECT
        ip.*,
        ROW_NUMBER() OVER (
            PARTITION BY ip.instrument_id, ip.price_date, ip.origin
            ORDER BY CASE WHEN ip.origin = 'MANUAL' THEN 0 ELSE 1 END,
                ip.price_id DESC
        ) AS row_rank
    FROM instrument_price_account_archive ip
)
WHERE row_rank = 1;

CREATE INDEX idx_price_instrument_date ON instrument_price(instrument_id, price_date);
CREATE UNIQUE INDEX uq_manual_instrument_price
ON instrument_price (instrument_id, price_date)
WHERE origin = 'MANUAL';

ALTER TABLE dividend_assumption RENAME TO dividend_assumption_account_archive;

CREATE TABLE dividend_assumption (
    assumption_id INTEGER PRIMARY KEY,
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
    UNIQUE (instrument_id, effective_date, source_row)
);

INSERT INTO dividend_assumption (
    assumption_id,
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
    assumption_id,
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
        da.*,
        ROW_NUMBER() OVER (
            PARTITION BY da.instrument_id, da.effective_date, da.source_row
            ORDER BY CASE WHEN da.origin = 'MANUAL' THEN 0 ELSE 1 END,
                da.assumption_id DESC
        ) AS row_rank
    FROM dividend_assumption_account_archive da
)
WHERE row_rank = 1;

CREATE INDEX idx_dividend_assumption_instrument_date
ON dividend_assumption(instrument_id, effective_date);
CREATE UNIQUE INDEX uq_manual_dividend_assumption
ON dividend_assumption (instrument_id, effective_date)
WHERE origin = 'MANUAL';

CREATE VIEW v_latest_instrument_price AS
SELECT *
FROM (
    SELECT
        ip.*,
        ROW_NUMBER() OVER (
            PARTITION BY ip.instrument_id
            ORDER BY ip.price_date DESC,
                CASE WHEN ip.origin = 'MANUAL' THEN 0 ELSE 1 END,
                ip.price_id DESC
        ) AS row_rank
    FROM instrument_price ip
)
WHERE row_rank = 1;

CREATE VIEW v_latest_dividend_assumption AS
SELECT *
FROM (
    SELECT
        da.*,
        ROW_NUMBER() OVER (
            PARTITION BY da.instrument_id
            ORDER BY da.effective_date DESC,
                CASE WHEN da.origin = 'MANUAL' THEN 0 ELSE 1 END,
                da.assumption_id DESC
        ) AS row_rank
    FROM dividend_assumption da
)
WHERE row_rank = 1;

CREATE VIEW IF NOT EXISTS v_latest_account_asset_snapshot AS
SELECT *
FROM (
    SELECT
        aas.*,
        ROW_NUMBER() OVER (
            PARTITION BY aas.account_id, aas.asset_type, aas.currency_code
            ORDER BY aas.snapshot_date DESC,
                CASE WHEN aas.origin = 'MANUAL' THEN 0 ELSE 1 END,
                aas.snapshot_id DESC
        ) AS row_rank
    FROM account_asset_snapshot aas
)
WHERE row_rank = 1;

CREATE VIEW v_latest_holding AS
SELECT *
FROM (
    SELECT
        hs.*,
        ROW_NUMBER() OVER (
            PARTITION BY hs.account_id, hs.instrument_id
            ORDER BY hs.snapshot_date DESC,
                CASE WHEN hs.origin = 'MANUAL' THEN 0 ELSE 1 END,
                hs.holding_snapshot_id DESC
        ) AS row_rank
    FROM holding_snapshot hs
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
       ON da.instrument_id = h.instrument_id
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
