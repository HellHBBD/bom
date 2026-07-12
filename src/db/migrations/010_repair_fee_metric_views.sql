DROP VIEW IF EXISTS v_asset_total;
DROP VIEW IF EXISTS v_holding_metrics;

CREATE VIEW v_holding_metrics AS
SELECT
    h.holding_snapshot_id, h.account_id, ao.person_id, p.display_name AS owner_name,
    h.instrument_id, i.symbol, i.name AS instrument_name, i.instrument_type,
    i.asset_class, i.region_type, i.trading_currency_code, h.snapshot_date,
    CAST(h.quantity_text AS REAL) AS quantity,
    CAST(h.average_cost_text AS REAL) AS average_cost,
    CAST(h.applied_buy_fee_rate AS REAL) AS buy_fee_rate,
    CAST(i.sell_fee_rate AS REAL) AS sell_fee_rate,
    CAST(i.sell_transaction_tax_rate AS REAL) AS sell_transaction_tax_rate,
    pr.price_date AS market_price_date, pr.currency_code AS market_price_currency_code,
    CAST(pr.price_text AS REAL) AS market_price,
    CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) AS total_cost,
    CAST(h.quantity_text AS REAL) * CAST(pr.price_text AS REAL) AS market_value,
    CAST(h.quantity_text AS REAL) * CAST(pr.price_text AS REAL)
        * (1 - CAST(i.sell_fee_rate AS REAL) - CAST(i.sell_transaction_tax_rate AS REAL)) AS liquidation_value,
    CAST(h.quantity_text AS REAL) * CAST(pr.price_text AS REAL)
        * (1 - CAST(i.sell_fee_rate AS REAL) - CAST(i.sell_transaction_tax_rate AS REAL))
        - CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) AS unrealized_profit,
    CASE WHEN CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) = 0 THEN NULL
        ELSE (CAST(h.quantity_text AS REAL) * CAST(pr.price_text AS REAL)
            * (1 - CAST(i.sell_fee_rate AS REAL) - CAST(i.sell_transaction_tax_rate AS REAL))
            - CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL))
            / (CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL))
    END AS unrealized_return_rate,
    da.effective_date AS dividend_effective_date, da.currency_code AS dividend_currency_code,
    CAST(da.estimated_annual_dividend_per_unit_text AS REAL) AS estimated_annual_dividend_per_unit,
    CAST(h.quantity_text AS REAL) * CAST(da.estimated_annual_dividend_per_unit_text AS REAL) AS estimated_annual_dividend,
    CASE WHEN CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL) = 0 THEN NULL
        ELSE (CAST(h.quantity_text AS REAL) * CAST(da.estimated_annual_dividend_per_unit_text AS REAL))
            / (CAST(h.quantity_text AS REAL) * CAST(h.average_cost_text AS REAL))
    END AS estimated_yield_on_cost,
    da.payments_per_year, CAST(da.latest_dividend_per_unit_text AS REAL) AS latest_dividend_per_unit
FROM v_latest_holding h
JOIN instrument i ON i.instrument_id = h.instrument_id
LEFT JOIN v_latest_instrument_price pr ON pr.instrument_id = h.instrument_id
LEFT JOIN v_latest_dividend_assumption da ON da.account_id = h.account_id AND da.instrument_id = h.instrument_id
LEFT JOIN account_owner ao ON ao.account_id = h.account_id
LEFT JOIN person p ON p.person_id = ao.person_id;

CREATE VIEW v_asset_total AS
SELECT p.person_id, p.display_name AS owner_name, 'ACCOUNT_ASSET' AS source_type,
    av.account_id, NULL AS instrument_id, av.current_value_ntd AS value_ntd
FROM v_account_asset_value av
JOIN account_owner ao ON ao.account_id = av.account_id
JOIN person p ON p.person_id = ao.person_id
UNION ALL
SELECT hm.person_id, hm.owner_name, 'HOLDING' AS source_type,
    hm.account_id, hm.instrument_id, hm.market_value AS value_ntd
FROM v_holding_metrics hm;

DROP TRIGGER IF EXISTS validate_instrument_fee_rates_insert;
DROP TRIGGER IF EXISTS validate_instrument_fee_rates_update;
DROP TRIGGER IF EXISTS validate_holding_snapshot_buy_fee_rate;
DROP TRIGGER IF EXISTS validate_holding_snapshot_buy_fee_rate_update;

CREATE TRIGGER validate_instrument_fee_rates_insert
BEFORE INSERT ON instrument
FOR EACH ROW
WHEN NEW.buy_fee_rate GLOB '*[^0-9.]*' OR NEW.buy_fee_rate IN ('', '.') OR NEW.buy_fee_rate GLOB '*.*.*'
  OR NEW.sell_fee_rate GLOB '*[^0-9.]*' OR NEW.sell_fee_rate IN ('', '.') OR NEW.sell_fee_rate GLOB '*.*.*'
  OR NEW.sell_transaction_tax_rate GLOB '*[^0-9.]*' OR NEW.sell_transaction_tax_rate IN ('', '.') OR NEW.sell_transaction_tax_rate GLOB '*.*.*'
BEGIN SELECT RAISE(ABORT, 'instrument fee rates must be non-negative decimal text'); END;

CREATE TRIGGER validate_instrument_fee_rates_update
BEFORE UPDATE OF buy_fee_rate, sell_fee_rate, sell_transaction_tax_rate ON instrument
FOR EACH ROW
WHEN NEW.buy_fee_rate GLOB '*[^0-9.]*' OR NEW.buy_fee_rate IN ('', '.') OR NEW.buy_fee_rate GLOB '*.*.*'
  OR NEW.sell_fee_rate GLOB '*[^0-9.]*' OR NEW.sell_fee_rate IN ('', '.') OR NEW.sell_fee_rate GLOB '*.*.*'
  OR NEW.sell_transaction_tax_rate GLOB '*[^0-9.]*' OR NEW.sell_transaction_tax_rate IN ('', '.') OR NEW.sell_transaction_tax_rate GLOB '*.*.*'
BEGIN SELECT RAISE(ABORT, 'instrument fee rates must be non-negative decimal text'); END;

CREATE TRIGGER validate_holding_snapshot_buy_fee_rate
BEFORE INSERT ON holding_snapshot
FOR EACH ROW
WHEN NEW.applied_buy_fee_rate GLOB '*[^0-9.]*'
  OR NEW.applied_buy_fee_rate IN ('', '.')
  OR NEW.applied_buy_fee_rate GLOB '*.*.*'
BEGIN SELECT RAISE(ABORT, 'snapshot buy fee rate must be non-negative decimal text'); END;

CREATE TRIGGER validate_holding_snapshot_buy_fee_rate_update
BEFORE UPDATE OF applied_buy_fee_rate ON holding_snapshot
FOR EACH ROW
WHEN NEW.applied_buy_fee_rate GLOB '*[^0-9.]*'
  OR NEW.applied_buy_fee_rate IN ('', '.')
  OR NEW.applied_buy_fee_rate GLOB '*.*.*'
BEGIN SELECT RAISE(ABORT, 'snapshot buy fee rate must be non-negative decimal text'); END;
