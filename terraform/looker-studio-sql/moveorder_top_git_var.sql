SELECT
    moveorder_short,
    SUM(sum_git_qty) git_qty,
    SUM(sum_git_value) git_val,
    ABS(SUM(sum_git_value)) git_val_abs
FROM `mm_fin_reporting.category_mo_var`(PARSE_DATE("%Y%m%d",@DS_START_DATE),PARSE_DATE("%Y%m%d",@DS_END_DATE),@category)
GROUP BY
    moveorder_short
ORDER BY
    git_val_abs DESC
;