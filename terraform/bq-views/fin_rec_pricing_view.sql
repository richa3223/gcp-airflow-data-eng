WITH sku_dates AS (
	SELECT pricing_date, sku, correlation_id, MAX(created_ts) AS ts
	FROM `mm_fin_internal.fin_rec_pricing`
	WHERE pricing_date > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    AND record_status = 'ACTIVE'
	GROUP BY pricing_date, sku, correlation_id
)
SELECT
    sd.pricing_date,
    sd.sku,
    sd.ts AS created_ts,
    p.correlation_id,
    p.record_status,
    p.min,
    p.pin,
    p.long_desc,
    p.room,
    p.room_two,
    p.trading_category,
    p.pack_weight,
    p.case_size,
    p.case_weight,
    p.rm,
    p.pack,
    p.lab,
    p.dist,
    p.oh,
    p.depot_loss,
    p.total,
    p.rm_case,
    p.pack_case,
    p.lab_case,
    p.dist_case,
    p.oh_case,
    p.depot_loss_case,
    p.total_case
FROM
	sku_dates sd,
	`mm_fin_internal.fin_rec_pricing` p
WHERE
	sd.ts = p.created_ts
AND sd.pricing_date = p.pricing_date
AND sd.sku = p.sku
and sd.correlation_id = p.correlation_id
ORDER BY sd.pricing_date DESC, sd.ts DESC
;