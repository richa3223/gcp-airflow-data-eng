SELECT
    depot_category,
    moveorder_short,
    SUM(sum_qty_variance) qty_variance,
    SUM(sum_val_variance) val_variance,
    ABS(SUM(sum_val_variance)) val_variance_abs
FROM `mm_fin_reporting.category_mo_var`(PARSE_DATE("%Y%m%d",@DS_START_DATE),PARSE_DATE("%Y%m%d",@DS_END_DATE),@category)
GROUP BY
    depot_category,
    moveorder_short
ORDER BY
    val_variance_abs DESC
;