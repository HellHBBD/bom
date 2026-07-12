ALTER TABLE instrument ADD COLUMN buy_fee_rate TEXT NOT NULL DEFAULT '0.001425'
    CHECK (
        CAST(buy_fee_rate AS REAL) >= 0
        AND CAST(buy_fee_rate AS REAL) < 1
    );

ALTER TABLE instrument ADD COLUMN sell_fee_rate TEXT NOT NULL DEFAULT '0'
    CHECK (
        CAST(sell_fee_rate AS REAL) >= 0
        AND CAST(sell_fee_rate AS REAL) < 1
    );

ALTER TABLE instrument ADD COLUMN sell_transaction_tax_rate TEXT NOT NULL DEFAULT '0'
    CHECK (
        CAST(sell_transaction_tax_rate AS REAL) >= 0
        AND CAST(sell_transaction_tax_rate AS REAL) < 1
        AND CAST(sell_fee_rate AS REAL) + CAST(sell_transaction_tax_rate AS REAL) < 1
    );
