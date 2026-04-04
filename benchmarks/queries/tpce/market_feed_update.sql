-- name: Market Feed — Update last trade prices
-- requires: in_subquery_memtable
-- description: Write portion of Market Feed: update last_trade with new prices from trade requests
-- timeout: 30s

-- Update the last trade price for securities that have pending trade requests.
-- In the full TPC-E profile, this processes a batch of market feed entries,
-- updating lt_price and lt_vol for each symbol.
UPDATE last_trade
SET lt_price = lt_price * 1.01,
    lt_vol = lt_vol + 100
WHERE lt_s_symb IN (
    SELECT DISTINCT tr.tr_s_symb
    FROM trade_request tr
);
