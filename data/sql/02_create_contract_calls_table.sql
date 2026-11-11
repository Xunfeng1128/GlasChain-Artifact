CREATE OR REPLACE TABLE
  `@@STAGING@@.glas_2026_5m_calls_20261018`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
AS
WITH selection AS (
  SELECT
    selectionSequence,
    blockNumber,
    transactionIndex,
    transactionHash,
    toIsContract,
    receiptStatus,
    gasUsed
  FROM `@@STAGING@@.glas_2026_5m_selection_20261018`
),
target_calls AS (
  SELECT
    s.selectionSequence,
    s.toIsContract AS rootToIsContract,
    s.receiptStatus,
    s.gasUsed AS transactionGasUsed,
    t.block_number,
    LOWER(t.transaction_hash) AS transaction_hash,
    s.transactionIndex AS transaction_index,
    t.trace_address,
    t.subtrace_count,
    t.action.from_address AS caller,
    t.action.to_address AS callee,
    t.action.call_type AS call_type,
    t.action.input AS call_input,
    t.action.value_lossless AS call_value_lossless,
    t.action.value AS call_value,
    t.action.gas AS call_gas_limit,
    t.result.gas_used AS call_gas_used,
    t.error
  FROM `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.traces` AS t
  JOIN selection AS s
    ON LOWER(t.transaction_hash) = s.transactionHash
   AND t.block_number = s.blockNumber
  WHERE t.block_timestamp >= TIMESTAMP('2026-08-14 00:00:00+00')
    AND t.block_timestamp < TIMESTAMP('2026-08-17 00:00:00+00')
    AND t.block_number BETWEEN 25755293 AND 25769799
    AND t.trace_type = 'call'
    AND t.transaction_hash IS NOT NULL
    AND t.action.from_address IS NOT NULL
    AND t.action.to_address IS NOT NULL
),
target_callees AS (
  SELECT DISTINCT LOWER(callee) AS address
  FROM target_calls
),
account_catalog AS (
  SELECT
    LOWER(a.address) AS address,
    LOGICAL_OR(LOWER(COALESCE(JSON_VALUE(a.code), a.code, '')) NOT IN ('', '0x')) AS is_contract
  FROM `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.accounts` AS a
  JOIN target_callees AS c
    ON LOWER(a.address) = c.address
  GROUP BY address
),
enriched AS (
  SELECT
    DIV(c.selectionSequence, 50000) AS epochId,
    c.block_number AS blockNumber,
    c.transaction_index AS transactionIndex,
    c.transaction_hash AS transactionHash,
    TO_JSON_STRING(c.trace_address) AS traceAddress,
    CASE
      WHEN ARRAY_LENGTH(c.trace_address) = 0 THEN ''
      ELSE TO_JSON_STRING(
        ARRAY(
          SELECT element
          FROM UNNEST(c.trace_address) AS element WITH OFFSET position
          WHERE position < ARRAY_LENGTH(c.trace_address) - 1
          ORDER BY position
        )
      )
    END AS parentTraceAddress,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT FORMAT('%010d', element)
        FROM UNNEST(c.trace_address) AS element WITH OFFSET position
        ORDER BY position
      ),
      '.'
    ) AS tracePathSortKey,
    ARRAY_LENGTH(c.trace_address) AS callDepth,
    c.subtrace_count AS subtraceCount,
    LOWER(c.caller) AS caller,
    LOWER(c.callee) AS callee,
    LOWER(c.call_type) AS callType,
    SUBSTR(COALESCE(c.call_input, ''), 1, 10) AS functionSelector,
    COALESCE(c.call_value_lossless, CAST(c.call_value AS STRING), '0') AS callValue,
    c.call_gas_limit AS callGasLimit,
    c.call_gas_used AS callGasUsed,
    c.error AS error,
    c.receiptStatus AS txStatus,
    c.transactionGasUsed,
    ARRAY_LENGTH(c.trace_address) > 0 AS callerIsContract,
    CASE
      WHEN ARRAY_LENGTH(c.trace_address) = 0 THEN c.rootToIsContract = 1
      ELSE COALESCE(a.is_contract, FALSE)
    END AS calleeIsContract,
    c.selectionSequence
  FROM target_calls AS c
  LEFT JOIN account_catalog AS a
    ON LOWER(c.callee) = a.address
),
final AS (
  SELECT
    epochId,
    blockNumber,
    transactionIndex,
    transactionHash,
    traceAddress,
    parentTraceAddress,
    tracePathSortKey,
    callDepth,
    subtraceCount,
    caller,
    callee,
    callType,
    functionSelector,
    callValue,
    callGasLimit,
    callGasUsed,
    error,
    txStatus,
    transactionGasUsed,
    callerIsContract,
    calleeIsContract,
    callerIsContract AND calleeIsContract AS isContractToContract,
    callerIsContract
      AND calleeIsContract
      AND callDepth > 0
      AND txStatus = 1
      AND error IS NULL AS isCCGEdge,
    selectionSequence
  FROM enriched
)
SELECT *
FROM final
ORDER BY selectionSequence, callDepth, tracePathSortKey;
