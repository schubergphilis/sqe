-- name: Market Feed
-- Read-only view of the pending market feed: trade requests awaiting execution,
-- joined with current last-trade data for price comparison.
-- Note: the full TPC-E Market Feed transaction also updates last_trade prices.

SELECT
    tr.tr_t_id                          AS request_id,
    tt.tt_name                          AS trade_type,
    tr.tr_s_symb                        AS symbol,
    tr.tr_qty                           AS requested_qty,
    tr.tr_bid_price                     AS requested_price,
    lt.lt_price                         AS current_price,
    tr.tr_bid_price - lt.lt_price       AS price_diff,
    b.b_name                            AS broker_name
FROM
    trade_request tr
    JOIN trade_type  tt ON tt.tt_id   = tr.tr_tt_id
    JOIN last_trade  lt ON lt.lt_s_symb = tr.tr_s_symb
    JOIN broker      b  ON b.b_id     = tr.tr_b_id
ORDER BY
    ABS(tr.tr_bid_price - lt.lt_price) DESC,
    tr.tr_t_id;
