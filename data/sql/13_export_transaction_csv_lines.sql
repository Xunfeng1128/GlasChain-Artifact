-- Serialize unchanged scalar columns as RFC 4180 CSV to reduce REST framing overhead.
SELECT ARRAY_TO_STRING(ARRAY(
  SELECT CASE
    WHEN v IS NULL THEN ''
    WHEN REGEXP_CONTAINS(v, r'[",\r\n]') THEN CONCAT('"', REPLACE(v, '"', '""'), '"')
    ELSE v
  END
  FROM UNNEST([
    CAST(t.`blockNumber` AS STRING),
    CAST(t.`timestamp` AS STRING),
    CAST(t.`transactionHash` AS STRING),
    CAST(t.`from` AS STRING),
    CAST(t.`to` AS STRING),
    CAST(t.`toCreate` AS STRING),
    CAST(t.`fromIsContract` AS STRING),
    CAST(t.`toIsContract` AS STRING),
    CAST(t.`value` AS STRING),
    CAST(t.`gasLimit` AS STRING),
    CAST(t.`gasPrice` AS STRING),
    CAST(t.`gasUsed` AS STRING),
    CAST(t.`callingFunction` AS STRING),
    CAST(t.`isError` AS STRING),
    CAST(t.`eip2718type` AS STRING),
    CAST(t.`baseFeePerGas` AS STRING),
    CAST(t.`maxFeePerGas` AS STRING),
    CAST(t.`maxPriorityFeePerGas` AS STRING),
    CAST(t.`reserved` AS STRING),
    CAST(t.`transactionIndex` AS STRING),
    CAST(t.`receiptStatus` AS STRING),
    CAST(t.`txCategory` AS STRING),
    CAST(t.`epochId` AS STRING),
    CAST(t.`internalCallCount` AS STRING),
    CAST(t.`contractCallCount` AS STRING),
    CAST(t.`maxCallDepth` AS STRING),
    CAST(t.`ccgVisibleGas` AS STRING),
    CAST(t.`hasCCGEdge` AS STRING),
    CAST(t.`sumCCGEdgeGasInclusive` AS STRING)
  ]) AS v WITH OFFSET position
  ORDER BY position
), ',') AS csv_line
FROM `@@STAGING@@.glas_2026_5m_transactions_20261018` AS t
ORDER BY selectionSequence;
