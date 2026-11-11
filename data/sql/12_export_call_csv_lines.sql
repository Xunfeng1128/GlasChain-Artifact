-- Serialize unchanged scalar columns as RFC 4180 CSV to reduce REST framing overhead.
SELECT ARRAY_TO_STRING(ARRAY(
  SELECT CASE
    WHEN v IS NULL THEN ''
    WHEN REGEXP_CONTAINS(v, r'[",\r\n]') THEN CONCAT('"', REPLACE(v, '"', '""'), '"')
    ELSE v
  END
  FROM UNNEST([
    CAST(t.`epochId` AS STRING),
    CAST(t.`blockNumber` AS STRING),
    CAST(t.`transactionIndex` AS STRING),
    CAST(t.`transactionHash` AS STRING),
    CAST(t.`traceAddress` AS STRING),
    CAST(t.`parentTraceAddress` AS STRING),
    CAST(t.`tracePathSortKey` AS STRING),
    CAST(t.`callDepth` AS STRING),
    CAST(t.`subtraceCount` AS STRING),
    CAST(t.`caller` AS STRING),
    CAST(t.`callee` AS STRING),
    CAST(t.`callType` AS STRING),
    CAST(t.`functionSelector` AS STRING),
    CAST(t.`callValue` AS STRING),
    CAST(t.`callGasLimit` AS STRING),
    CAST(t.`callGasUsed` AS STRING),
    CAST(t.`error` AS STRING),
    CAST(t.`txStatus` AS STRING),
    CAST(t.`transactionGasUsed` AS STRING),
    CAST(t.`callerIsContract` AS STRING),
    CAST(t.`calleeIsContract` AS STRING),
    CAST(t.`isContractToContract` AS STRING),
    CAST(t.`isCCGEdge` AS STRING)
  ]) AS v WITH OFFSET position
  ORDER BY position
), ',') AS csv_line
FROM `@@STAGING@@.glas_2026_5m_calls_20261018` AS t
ORDER BY selectionSequence, callDepth, tracePathSortKey;
