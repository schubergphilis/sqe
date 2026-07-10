-- TPC-E Schema DDL
-- Stock brokerage benchmark with 33 tables across 4 domains + reference data.
-- Scale factor = number of customers (default 1,000 per SF unit).
-- Table names are unqualified; the benchmark runner prepends the namespace.

-- ============================================================
-- Reference tables (fixed size, independent of scale factor)
-- ============================================================

CREATE TABLE status_type (
    st_id       VARCHAR(4)   NOT NULL,
    st_name     VARCHAR(10)  NOT NULL
);

CREATE TABLE trade_type (
    tt_id       VARCHAR(3)   NOT NULL,
    tt_name     VARCHAR(12)  NOT NULL,
    tt_is_sell  INTEGER      NOT NULL,
    tt_is_mrkt  INTEGER      NOT NULL
);

CREATE TABLE exchange (
    ex_id       VARCHAR(6)   NOT NULL,
    ex_name     VARCHAR(100) NOT NULL,
    ex_num_symb INTEGER      NOT NULL,
    ex_open     INTEGER      NOT NULL,
    ex_close    INTEGER      NOT NULL,
    ex_desc     VARCHAR(150),
    ex_ad_id    BIGINT       NOT NULL
);

CREATE TABLE sector (
    sc_id       VARCHAR(6)   NOT NULL,
    sc_name     VARCHAR(30)  NOT NULL
);

CREATE TABLE industry (
    in_id       VARCHAR(6)   NOT NULL,
    in_name     VARCHAR(50)  NOT NULL,
    in_sc_id    VARCHAR(6)   NOT NULL
);

CREATE TABLE taxrate (
    tx_id       VARCHAR(4)   NOT NULL,
    tx_name     VARCHAR(50)  NOT NULL,
    tx_rate     DECIMAL(6,5) NOT NULL
);

CREATE TABLE commission_rate (
    cr_c_tier    INTEGER      NOT NULL,
    cr_tt_id     VARCHAR(3)   NOT NULL,
    cr_ex_id     VARCHAR(6)   NOT NULL,
    cr_from_qty  INTEGER      NOT NULL,
    cr_to_qty    INTEGER      NOT NULL,
    cr_rate      DECIMAL(7,5) NOT NULL
);

CREATE TABLE charge (
    ch_tt_id    VARCHAR(3)    NOT NULL,
    ch_chrg     DECIMAL(10,2) NOT NULL
);

-- zip_code: 14,741 fixed rows
CREATE TABLE zip_code (
    zc_code     VARCHAR(5)   NOT NULL,
    zc_town     VARCHAR(80)  NOT NULL,
    zc_div      VARCHAR(80)  NOT NULL
);

-- ============================================================
-- Reference tables (scale-dependent)
-- ============================================================

-- SF × 5,500
CREATE TABLE address (
    ad_id       BIGINT       NOT NULL,
    ad_line1    VARCHAR(80)  NOT NULL,
    ad_line2    VARCHAR(80),
    ad_zc_code  VARCHAR(5)   NOT NULL,
    ad_ctry     VARCHAR(80)
);

-- ============================================================
-- Customer domain
-- ============================================================

-- SF × 1,000
CREATE TABLE customer (
    c_id        BIGINT       NOT NULL,
    c_tax_id    VARCHAR(20)  NOT NULL,
    c_st_id     VARCHAR(4)   NOT NULL,
    c_l_name    VARCHAR(25)  NOT NULL,
    c_f_name    VARCHAR(20)  NOT NULL,
    c_m_name    VARCHAR(1),
    c_gndr      VARCHAR(1),
    c_tier      INTEGER      NOT NULL,
    c_dob       DATE         NOT NULL,
    c_ad_id     BIGINT       NOT NULL,
    c_ctry_1    VARCHAR(3),
    c_area_1    VARCHAR(3),
    c_local_1   VARCHAR(10),
    c_ext_1     VARCHAR(5),
    c_email_1   VARCHAR(50),
    c_email_2   VARCHAR(50)
);

-- SF × 5
CREATE TABLE customer_account (
    ca_id       BIGINT        NOT NULL,
    ca_b_id     BIGINT        NOT NULL,
    ca_c_id     BIGINT        NOT NULL,
    ca_name     VARCHAR(50),
    ca_tax_st   INTEGER       NOT NULL,
    ca_bal      DECIMAL(12,2) NOT NULL
);

-- SF × 2,000
CREATE TABLE customer_taxrate (
    cx_tx_id    VARCHAR(4)   NOT NULL,
    cx_c_id     BIGINT       NOT NULL
);

-- SF × 5,000
CREATE TABLE account_permission (
    ap_ca_id    BIGINT       NOT NULL,
    ap_acl      VARCHAR(4)   NOT NULL,
    ap_tax_id   VARCHAR(20)  NOT NULL,
    ap_l_name   VARCHAR(25)  NOT NULL,
    ap_f_name   VARCHAR(20)  NOT NULL
);

-- SF × 12,500
CREATE TABLE holding (
    h_t_id      BIGINT        NOT NULL,
    h_ca_id     BIGINT        NOT NULL,
    h_s_symb    VARCHAR(15)   NOT NULL,
    h_dts       DATE          NOT NULL,
    h_price     DECIMAL(8,2)  NOT NULL,
    h_qty       INTEGER       NOT NULL
);

-- SF × 25,000
CREATE TABLE holding_history (
    hh_h_t_id     BIGINT   NOT NULL,
    hh_t_id       BIGINT   NOT NULL,
    hh_before_qty INTEGER  NOT NULL,
    hh_after_qty  INTEGER  NOT NULL
);

-- SF × 5,000
CREATE TABLE holding_summary (
    hs_ca_id    BIGINT       NOT NULL,
    hs_s_symb   VARCHAR(15)  NOT NULL,
    hs_qty      INTEGER      NOT NULL
);

-- SF × 5,000
CREATE TABLE watch_list (
    wl_id       BIGINT   NOT NULL,
    wl_c_id     BIGINT   NOT NULL
);

-- SF × 50,000
CREATE TABLE watch_item (
    wi_wl_id    BIGINT       NOT NULL,
    wi_s_symb   VARCHAR(15)  NOT NULL
);

-- ============================================================
-- Broker domain
-- ============================================================

-- SF × 10
CREATE TABLE broker (
    b_id         BIGINT        NOT NULL,
    b_st_id      VARCHAR(4)    NOT NULL,
    b_name       VARCHAR(49)   NOT NULL,
    b_num_trades INTEGER       NOT NULL,
    b_comm_total DECIMAL(12,2) NOT NULL
);

-- ============================================================
-- Market domain
-- ============================================================

-- SF × 17,280
CREATE TABLE trade (
    t_id          BIGINT        NOT NULL,
    t_dts         DATE          NOT NULL,
    t_st_id       VARCHAR(4)    NOT NULL,
    t_tt_id       VARCHAR(3)    NOT NULL,
    t_is_cash     INTEGER       NOT NULL,
    t_s_symb      VARCHAR(15)   NOT NULL,
    t_qty         INTEGER       NOT NULL,
    t_bid_price   DECIMAL(8,2)  NOT NULL,
    t_ca_id       BIGINT        NOT NULL,
    t_exec_name   VARCHAR(49)   NOT NULL,
    t_trade_price DECIMAL(8,2),
    t_chrg        DECIMAL(10,2) NOT NULL,
    t_comm        DECIMAL(10,2),
    t_tax         DECIMAL(10,2),
    t_lifo        INTEGER       NOT NULL
);

-- SF × 51,840
CREATE TABLE trade_history (
    th_t_id     BIGINT      NOT NULL,
    th_dts      DATE        NOT NULL,
    th_st_id    VARCHAR(4)  NOT NULL
);

-- SF × 100 (pending requests)
CREATE TABLE trade_request (
    tr_t_id       BIGINT       NOT NULL,
    tr_tt_id      VARCHAR(3)   NOT NULL,
    tr_s_symb     VARCHAR(15)  NOT NULL,
    tr_qty        INTEGER      NOT NULL,
    tr_bid_price  DECIMAL(8,2) NOT NULL,
    tr_b_id       BIGINT       NOT NULL
);

-- SF × 17,280
CREATE TABLE settlement (
    se_t_id          BIGINT        NOT NULL,
    se_cash_type     VARCHAR(40)   NOT NULL,
    se_cash_due_date DATE          NOT NULL,
    se_amt           DECIMAL(10,2) NOT NULL
);

-- SF × 13,824
CREATE TABLE cash_transaction (
    ct_t_id     BIGINT        NOT NULL,
    ct_dts      DATE          NOT NULL,
    ct_amt      DECIMAL(10,2) NOT NULL,
    ct_name     VARCHAR(100)
);

-- ============================================================
-- Company domain
-- ============================================================

-- SF × 5
CREATE TABLE company (
    co_id         BIGINT       NOT NULL,
    co_st_id      VARCHAR(4)   NOT NULL,
    co_name       VARCHAR(60)  NOT NULL,
    co_in_id      VARCHAR(6)   NOT NULL,
    co_sp_rate    VARCHAR(4)   NOT NULL,
    co_ceo        VARCHAR(46)  NOT NULL,
    co_ad_id      BIGINT       NOT NULL,
    co_desc       VARCHAR(150) NOT NULL,
    co_open_date  DATE         NOT NULL
);

-- SF × 15
CREATE TABLE company_competitor (
    cp_co_id      BIGINT      NOT NULL,
    cp_comp_co_id BIGINT      NOT NULL,
    cp_in_id      VARCHAR(6)  NOT NULL
);

-- SF × 6.85
CREATE TABLE security (
    s_symb            VARCHAR(15)   NOT NULL,
    s_issue           VARCHAR(6)    NOT NULL,
    s_st_id           VARCHAR(4)    NOT NULL,
    s_name            VARCHAR(70)   NOT NULL,
    s_ex_id           VARCHAR(6)    NOT NULL,
    s_co_id           BIGINT        NOT NULL,
    s_num_out         BIGINT        NOT NULL,
    s_start_date      DATE          NOT NULL,
    s_exch_date       DATE          NOT NULL,
    s_pe              DECIMAL(10,2) NOT NULL,
    s_52wk_high       DECIMAL(8,2)  NOT NULL,
    s_52wk_high_date  DATE          NOT NULL,
    s_52wk_low        DECIMAL(8,2)  NOT NULL,
    s_52wk_low_date   DATE          NOT NULL,
    s_dividend        DECIMAL(10,2) NOT NULL,
    s_yield           DECIMAL(5,2)  NOT NULL
);

-- SF × 17,136
CREATE TABLE daily_market (
    dm_date     DATE          NOT NULL,
    dm_s_symb   VARCHAR(15)   NOT NULL,
    dm_close    DECIMAL(8,2)  NOT NULL,
    dm_high     DECIMAL(8,2)  NOT NULL,
    dm_low      DECIMAL(8,2)  NOT NULL,
    dm_vol      BIGINT        NOT NULL
);

-- SF × 100
CREATE TABLE financial (
    fi_co_id          BIGINT        NOT NULL,
    fi_year           INTEGER       NOT NULL,
    fi_qtr            INTEGER       NOT NULL,
    fi_qtr_start_date DATE          NOT NULL,
    fi_revenue        DECIMAL(15,2) NOT NULL,
    fi_net_earn       DECIMAL(15,2) NOT NULL,
    fi_basic_eps      DECIMAL(10,2) NOT NULL,
    fi_dilut_eps      DECIMAL(10,2) NOT NULL,
    fi_margin         DECIMAL(10,4) NOT NULL,
    fi_inventory      DECIMAL(15,2) NOT NULL,
    fi_assets         DECIMAL(15,2) NOT NULL,
    fi_liability      DECIMAL(15,2) NOT NULL,
    fi_out_basic      BIGINT        NOT NULL,
    fi_out_dilut      BIGINT        NOT NULL
);

-- SF × 6.85 (one row per security)
CREATE TABLE last_trade (
    lt_s_symb     VARCHAR(15)   NOT NULL,
    lt_dts        DATE          NOT NULL,
    lt_price      DECIMAL(8,2)  NOT NULL,
    lt_open_price DECIMAL(8,2)  NOT NULL,
    lt_vol        BIGINT        NOT NULL
);

-- SF × 100
CREATE TABLE news_item (
    ni_id       BIGINT        NOT NULL,
    ni_headline VARCHAR(80)   NOT NULL,
    ni_summary  VARCHAR(255)  NOT NULL,
    ni_item     VARCHAR(1000) NOT NULL,
    ni_dts      DATE          NOT NULL,
    ni_source   VARCHAR(30)   NOT NULL,
    ni_author   VARCHAR(30)
);

-- SF × 100
CREATE TABLE news_xref (
    nx_ni_id    BIGINT   NOT NULL,
    nx_co_id    BIGINT   NOT NULL
);
