UPDATE instrument SET symbol = '00882', name = '中信中國高股息' WHERE instrument_id = 1 AND symbol = '142';
UPDATE instrument SET symbol = '4137', name = '麗豐-KY' WHERE instrument_id = 2 AND symbol = '144';
UPDATE instrument SET symbol = '2474' WHERE instrument_id = 3 AND symbol = '146';
UPDATE instrument SET symbol = '5871', name = '中租-KY' WHERE instrument_id = 4 AND symbol = '148';
UPDATE instrument SET symbol = '8341' WHERE instrument_id = 5 AND symbol = '150';
UPDATE instrument SET symbol = '2201' WHERE instrument_id = 6 AND symbol = '152';
UPDATE instrument SET symbol = '2258', name = '鴻華先進-創' WHERE instrument_id = 7 AND symbol = '154';
UPDATE instrument SET symbol = '2352', name = '佳世達' WHERE instrument_id = 8 AND symbol = '156';
UPDATE instrument SET symbol = '7723' WHERE instrument_id = 9 AND symbol = '158';
UPDATE instrument SET symbol = '3130' WHERE instrument_id = 10 AND symbol = '160';
UPDATE instrument SET symbol = '6776', name = '展碁國際' WHERE instrument_id = 11 AND symbol = '162';
UPDATE instrument SET symbol = '1707' WHERE instrument_id = 12 AND symbol = '164';
UPDATE instrument SET symbol = '00712' WHERE instrument_id = 13 AND symbol = '167';
UPDATE instrument SET symbol = '00945B' WHERE instrument_id = 14 AND symbol = '169';
UPDATE instrument SET symbol = '00679B' WHERE instrument_id = 15 AND symbol = '171';
UPDATE instrument SET symbol = '00687B' WHERE instrument_id = 16 AND symbol = '173';
UPDATE instrument SET symbol = '00720B' WHERE instrument_id = 17 AND symbol = '175';
UPDATE instrument SET symbol = '00722B' WHERE instrument_id = 18 AND symbol = '177';
UPDATE instrument SET symbol = '00724B' WHERE instrument_id = 19 AND symbol = '179';
UPDATE instrument SET symbol = '00725B' WHERE instrument_id = 20 AND symbol = '181';
UPDATE instrument SET symbol = '00740B' WHERE instrument_id = 21 AND symbol = '183';
UPDATE instrument SET symbol = '00751B' WHERE instrument_id = 22 AND symbol = '185';
UPDATE instrument SET symbol = '00772B' WHERE instrument_id = 23 AND symbol = '187';
UPDATE instrument SET symbol = '00773B' WHERE instrument_id = 24 AND symbol = '189';
UPDATE instrument SET symbol = '00795B' WHERE instrument_id = 25 AND symbol = '191';
UPDATE instrument SET symbol = '4115', name = '善德生技' WHERE instrument_id = 26 AND symbol = '193';
UPDATE instrument SET symbol = '1264' WHERE instrument_id = 27 AND symbol = '195';
UPDATE instrument SET symbol = '9911' WHERE instrument_id = 28 AND symbol = '197';
UPDATE instrument SET symbol = '00878', name = '國泰永續高股息' WHERE instrument_id = 33 AND symbol = '207';
UPDATE instrument SET symbol = '1733' WHERE instrument_id = 34 AND symbol = '209';
UPDATE instrument SET symbol = '6534', name = '正瀚-創' WHERE instrument_id = 35 AND symbol = '211';

UPDATE instrument
SET symbol = UPPER(TRIM(symbol))
WHERE symbol IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_symbol
ON instrument (UPPER(TRIM(symbol)))
WHERE NULLIF(TRIM(symbol), '') IS NOT NULL;

DROP TRIGGER IF EXISTS validate_instrument_symbol_insert;
DROP TRIGGER IF EXISTS validate_instrument_symbol_update;

CREATE TRIGGER validate_instrument_symbol_insert
BEFORE INSERT ON instrument
FOR EACH ROW
WHEN (NEW.instrument_type IN ('STOCK', 'ETF') AND NULLIF(TRIM(NEW.symbol), '') IS NULL)
  OR (NEW.symbol IS NOT NULL AND NEW.symbol <> UPPER(TRIM(NEW.symbol)))
BEGIN SELECT RAISE(ABORT, 'stock and ETF symbols must be uppercase and non-empty'); END;

CREATE TRIGGER validate_instrument_symbol_update
BEFORE UPDATE OF symbol, instrument_type ON instrument
FOR EACH ROW
WHEN (NEW.instrument_type IN ('STOCK', 'ETF') AND NULLIF(TRIM(NEW.symbol), '') IS NULL)
  OR (NEW.symbol IS NOT NULL AND NEW.symbol <> UPPER(TRIM(NEW.symbol)))
BEGIN SELECT RAISE(ABORT, 'stock and ETF symbols must be uppercase and non-empty'); END;
