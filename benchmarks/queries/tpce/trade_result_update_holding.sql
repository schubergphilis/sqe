-- name: Trade Result — Update holding summary
-- requires: full_schema
-- description: Write portion of Trade Result: update holding_summary quantities for settled trades
-- timeout: 30s

-- The Trade Result transaction updates holding_summary after a trade settles.
-- For buy trades the summary qty increases; for sell trades it decreases.
-- This simplified version updates summaries for all pending trades.
UPDATE holding_summary
SET hs_qty = hs_qty + (
    SELECT CASE WHEN tt.tt_is_sell THEN -t.t_qty ELSE t.t_qty END
    FROM trade t
    JOIN trade_type tt ON tt.tt_id = t.t_tt_id
    WHERE t.t_ca_id = holding_summary.hs_ca_id
      AND t.t_s_symb = holding_summary.hs_s_symb
      AND t.t_st_id = 'PNDG'
    LIMIT 1
)
WHERE (hs_ca_id, hs_s_symb) IN (
    SELECT t.t_ca_id, t.t_s_symb
    FROM trade t
    WHERE t.t_st_id = 'PNDG'
);
