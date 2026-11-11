CREATE OR REPLACE TABLE
  `@@STAGING@@.glas_2026_5m_selection_20261018`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
AS
WITH account_catalog AS (
  SELECT
    LOWER(address) AS address,
    LOGICAL_OR(is_contract) AS is_contract
  FROM `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.accounts`
  GROUP BY address
),
eligible AS (
  SELECT
    t.block_number AS blockNumber,
    UNIX_SECONDS(t.block_timestamp) AS timestamp,
    LOWER(t.transaction_hash) AS transactionHash,
    LOWER(t.from_address) AS `from`,
    LOWER(t.to_address) AS `to`,
    CAST(NULL AS STRING) AS toCreate,
    0 AS fromIsContract,
    IF(COALESCE(a.is_contract, FALSE), 1, 0) AS toIsContract,
    COALESCE(t.value_lossless, CAST(t.value AS STRING), '0') AS value,
    t.gas AS gasLimit,
    t.gas_price AS gasPrice,
    r.gas_used AS gasUsed,
    SUBSTR(COALESCE(t.input, ''), 1, 10) AS callingFunction,
    CAST(NULL AS STRING) AS isError,
    t.transaction_type AS eip2718type,
    b.base_fee_per_gas AS baseFeePerGas,
    t.max_fee_per_gas AS maxFeePerGas,
    t.max_priority_fee_per_gas AS maxPriorityFeePerGas,
    r.transaction_index AS transactionIndex,
    r.status AS receiptStatus,
    ROW_NUMBER() OVER (
      ORDER BY t.block_number, r.transaction_index, LOWER(t.transaction_hash)
    ) - 1 AS selectionSequence
  FROM `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.transactions` AS t
  JOIN `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.receipts` AS r
    ON t.block_number = r.block_number
   AND t.transaction_hash = r.transaction_hash
  JOIN `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.blocks` AS b
    ON t.block_number = b.block_number
  LEFT JOIN account_catalog AS a
    ON LOWER(t.to_address) = a.address
  WHERE t.block_timestamp >= TIMESTAMP('2026-08-14 00:00:00+00')
    AND t.block_timestamp < TIMESTAMP('2026-08-19 00:00:00+00')
    AND r.block_timestamp >= TIMESTAMP('2026-08-14 00:00:00+00')
    AND r.block_timestamp < TIMESTAMP('2026-08-19 00:00:00+00')
    AND b.block_timestamp >= TIMESTAMP('2026-08-14 00:00:00+00')
    AND b.block_timestamp < TIMESTAMP('2026-08-19 00:00:00+00')
    AND t.block_number BETWEEN 25755293 AND 25775292
    AND r.block_number BETWEEN 25755293 AND 25775292
    AND b.block_number BETWEEN 25755293 AND 25775292
    AND r.status = 1
    AND t.from_address IS NOT NULL
    AND t.to_address IS NOT NULL
    AND LOWER(t.from_address) != LOWER(t.to_address)
),
selected AS (
  SELECT *
  FROM eligible
  WHERE selectionSequence < 5000000
)
SELECT *
FROM selected
ORDER BY selectionSequence;
