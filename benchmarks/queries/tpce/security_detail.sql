-- name: Security Detail
-- Full security profile: security details joined with company info,
-- financials summary, and recent daily market data.

SELECT
    s.s_symb                                AS symbol,
    s.s_name                                AS security_name,
    s.s_issue                               AS issue_type,
    ex.ex_name                              AS exchange_name,
    co.co_name                              AS company_name,
    co.co_desc                              AS company_desc,
    co.co_ceo                               AS ceo,
    ind.in_name                             AS industry,
    sec.sc_name                             AS sector,
    s.s_pe                                  AS pe_ratio,
    s.s_52wk_high                           AS wk52_high,
    s.s_52wk_low                            AS wk52_low,
    s.s_dividend                            AS dividend,
    s.s_yield                               AS yield,
    s.s_num_out                             AS shares_outstanding,
    f.fi_year                               AS fiscal_year,
    f.fi_qtr                                AS fiscal_qtr,
    f.fi_revenue                            AS revenue,
    f.fi_net_earn                           AS net_earnings,
    f.fi_basic_eps                          AS basic_eps,
    f.fi_margin                             AS margin
FROM
    security     s
    JOIN exchange    ex  ON ex.ex_id    = s.s_ex_id
    JOIN company     co  ON co.co_id    = s.s_co_id
    JOIN industry    ind ON ind.in_id   = co.co_in_id
    JOIN sector      sec ON sec.sc_id   = ind.in_sc_id
    LEFT JOIN financial f ON f.fi_co_id  = co.co_id
ORDER BY
    s.s_symb,
    f.fi_year DESC,
    f.fi_qtr DESC;
