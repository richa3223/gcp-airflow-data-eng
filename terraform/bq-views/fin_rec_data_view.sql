WITH fingerprint_ts AS (
    SELECT
        record_date,
        fingerprint,
        MAX(created_ts) AS ts
    FROM `mm_fin_internal.fin_rec_data`
    WHERE record_date > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    AND record_status = 'ACTIVE'    
    AND valid_from >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)
    GROUP BY      
        record_date,      
        fingerprint
    ORDER BY
      record_date,
      fingerprint,
      ts DESC
)
SELECT
  fpts.ts AS created_ts,
  frd.correlation_id,
  frd.record_status,
  frd.record_date,
  frd.source_data_type,
  frd.sku,
  frd.moveorder_short,
  frd.lot_number,
  frd.depot_id,
  frd.depot_name,
  frd.depot_category,
  frd.sku_moveorder,
  frd.order_id,
  frd.sku_and_order,
  frd.pkrd_unit_price,
  frd.pkrd_case_price,
  frd.pkrd_quantity,
  frd.pkrd_value,
  frd.pkrd_value_tp,
  frd.nfsi_quantity,
  frd.nfsi_value,
  frd.quantity_variance,
  frd.value_variance,
  frd.value_variance_tp,
  frd.fingerprint
FROM  
  fingerprint_ts fpts,
  `mm_fin_internal.fin_rec_data` frd
WHERE fpts.record_date = frd.record_date
AND fpts.fingerprint = frd.fingerprint
AND fpts.ts = frd.created_ts
;