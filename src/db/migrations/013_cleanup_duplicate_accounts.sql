UPDATE account_asset_snapshot
SET account_id = CASE account_id
    WHEN 29 THEN 27
    WHEN 34 THEN 30
    WHEN 36 THEN 30
END
WHERE account_id IN (29, 34, 36);

DELETE FROM account_asset_snapshot
WHERE account_id IN (15, 16, 17, 18, 19);

DELETE FROM account_owner
WHERE account_id IN (15, 16, 17, 18, 19, 29, 34, 36);

DELETE FROM account
WHERE account_id IN (15, 16, 17, 18, 19, 29, 34, 36);

DELETE FROM institution
WHERE institution_id IN (8, 9, 10, 11, 12)
  AND NOT EXISTS (
      SELECT 1
      FROM account
      WHERE account.institution_id = institution.institution_id
  );
