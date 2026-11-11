CREATE OR REPLACE TABLE
  `@@STAGING@@.glas_2026_5m_transactions_20261018`
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
AS
WITH call_stats AS (
  SELECT
    selectionSequence,
    COUNTIF(callDepth > 0) AS internalCallCount,
    COUNTIF(isCCGEdge) AS contractCallCount,
    MAX(callDepth) AS maxCallDepth,
    SUM(IF(isCCGEdge AND callDepth = 1, COALESCE(callGasUsed, 0), 0)) AS ccgVisibleGas,
    SUM(IF(isCCGEdge, COALESCE(callGasUsed, 0), 0)) AS sumCCGEdgeGasInclusive
  FROM `@@STAGING@@.glas_2026_5m_calls_20261018`
  GROUP BY selectionSequence
)
SELECT
  s.* EXCEPT(transactionIndex, receiptStatus, selectionSequence),
  CAST(NULL AS STRING) AS reserved,
  s.transactionIndex,
  s.receiptStatus,
  CASE
    WHEN COALESCE(c.contractCallCount, 0) > 0 THEN 'inter_contract_call'
    WHEN s.toIsContract = 1 THEN 'single_contract_call'
    ELSE 'transfer'
  END AS txCategory,
  DIV(s.selectionSequence, 50000) AS epochId,
  COALESCE(c.internalCallCount, 0) AS internalCallCount,
  COALESCE(c.contractCallCount, 0) AS contractCallCount,
  COALESCE(c.maxCallDepth, 0) AS maxCallDepth,
  COALESCE(c.ccgVisibleGas, 0) AS ccgVisibleGas,
  COALESCE(c.contractCallCount, 0) > 0 AS hasCCGEdge,
  COALESCE(c.sumCCGEdgeGasInclusive, 0) AS sumCCGEdgeGasInclusive,
  s.selectionSequence
FROM `@@STAGING@@.glas_2026_5m_selection_20261018` AS s
LEFT JOIN call_stats AS c USING (selectionSequence);
