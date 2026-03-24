-- name: Data Maintenance
-- requires: update, delete
-- Read portion of the Data Maintenance transaction: identify stale or
-- candidate rows across reference and transactional tables for maintenance.
-- Note: the full TPC-E Data Maintenance transaction updates and deletes rows.

SELECT
    'news_item'                         AS target_table,
    ni.ni_id                            AS row_id,
    ni.ni_dts                           AS row_date,
    ni.ni_source                        AS detail
FROM
    news_item ni
WHERE
    ni.ni_dts < DATE '2001-01-01'

UNION ALL

SELECT
    'daily_market'                      AS target_table,
    CAST(dm.dm_vol AS BIGINT)           AS row_id,
    dm.dm_date                          AS row_date,
    dm.dm_s_symb                        AS detail
FROM
    daily_market dm
WHERE
    dm.dm_vol = 0

UNION ALL

SELECT
    'holding_history'                   AS target_table,
    hh.hh_h_t_id                        AS row_id,
    CAST(NULL AS DATE)                  AS row_date,
    CAST(hh.hh_before_qty AS VARCHAR)   AS detail
FROM
    holding_history hh
WHERE
    hh.hh_before_qty = hh.hh_after_qty

ORDER BY
    target_table,
    row_id;
