CREATE TRIGGER IF NOT EXISTS validate_instrument_fee_rates_insert
BEFORE INSERT ON instrument
FOR EACH ROW
WHEN NEW.buy_fee_rate GLOB '*[^0-9.]*'
  OR NEW.buy_fee_rate = ''
  OR NEW.buy_fee_rate = '.'
  OR NEW.buy_fee_rate GLOB '*.*.*'
  OR NEW.sell_fee_rate GLOB '*[^0-9.]*'
  OR NEW.sell_fee_rate = ''
  OR NEW.sell_fee_rate = '.'
  OR NEW.sell_fee_rate GLOB '*.*.*'
  OR NEW.sell_transaction_tax_rate GLOB '*[^0-9.]*'
  OR NEW.sell_transaction_tax_rate = ''
  OR NEW.sell_transaction_tax_rate = '.'
  OR NEW.sell_transaction_tax_rate GLOB '*.*.*'
BEGIN
    SELECT RAISE(ABORT, 'instrument fee rates must be non-negative decimal text');
END;

CREATE TRIGGER IF NOT EXISTS validate_instrument_fee_rates_update
BEFORE UPDATE OF buy_fee_rate, sell_fee_rate, sell_transaction_tax_rate ON instrument
FOR EACH ROW
WHEN NEW.buy_fee_rate GLOB '*[^0-9.]*'
  OR NEW.buy_fee_rate = ''
  OR NEW.buy_fee_rate = '.'
  OR NEW.buy_fee_rate GLOB '*.*.*'
  OR NEW.sell_fee_rate GLOB '*[^0-9.]*'
  OR NEW.sell_fee_rate = ''
  OR NEW.sell_fee_rate = '.'
  OR NEW.sell_fee_rate GLOB '*.*.*'
  OR NEW.sell_transaction_tax_rate GLOB '*[^0-9.]*'
  OR NEW.sell_transaction_tax_rate = ''
  OR NEW.sell_transaction_tax_rate = '.'
  OR NEW.sell_transaction_tax_rate GLOB '*.*.*'
BEGIN
    SELECT RAISE(ABORT, 'instrument fee rates must be non-negative decimal text');
END;
