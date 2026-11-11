-- The current accounts view marks even JSON-encoded empty bytecode ("0x") as is_contract=true.
-- Reconstruct the intended nonempty-bytecode predicate; retain the already-selected
-- identities, canonical order, receipts and actual observed transaction Gas.
CREATE OR REPLACE TABLE
 `@@STAGING@@.glas_2026_5m_selection_20261018`
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
AS
WITH account_catalog AS (
 SELECT LOWER(address) AS address,
 LOGICAL_OR(LOWER(COALESCE(JSON_VALUE(code), code, '')) NOT IN ('', '0x')) AS is_contract
 FROM `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.accounts`
 GROUP BY address
)
SELECT s.* REPLACE(IF(COALESCE(a.is_contract,FALSE),1,0) AS toIsContract)
FROM `@@STAGING@@.glas_2026_5m_selection_20261018` AS s
LEFT JOIN account_catalog a ON s.`to`=a.address;
