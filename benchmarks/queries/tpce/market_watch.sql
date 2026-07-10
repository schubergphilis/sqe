-- name: Market Watch
-- Market data for securities on a customer's watch lists: last trade price,
-- 52-week high/low, and daily volume for watched symbols.

SELECT
    wi.wi_s_symb                        AS symbol,
    s.s_name                            AS security_name,
    lt.lt_price                         AS last_price,
    lt.lt_open_price                    AS open_price,
    lt.lt_price - lt.lt_open_price      AS price_change,
    s.s_52wk_high                       AS wk52_high,
    s.s_52wk_low                        AS wk52_low,
    lt.lt_vol                           AS last_vol,
    s.s_yield                           AS yield,
    s.s_dividend                        AS dividend
FROM
    watch_item  wi
    JOIN watch_list  wl ON wl.wl_id     = wi.wi_wl_id
    JOIN security    s  ON s.s_symb     = wi.wi_s_symb
    JOIN last_trade  lt ON lt.lt_s_symb = wi.wi_s_symb
ORDER BY
    wl.wl_c_id,
    symbol;
