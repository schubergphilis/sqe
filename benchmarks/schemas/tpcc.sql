-- TPC-C Schema DDL
-- Standard TPC-C tables. Scale factor = number of warehouses.
-- Table names are unqualified; the benchmark runner prepends the namespace.
--
-- Row counts per warehouse (scale factor W):
--   warehouse:   W rows
--   district:    W * 10 rows
--   customer:    W * 30,000 rows
--   hist:        W * 30,000 rows
--   orders:      W * 30,000 rows
--   new_order:   W * 9,000 rows   (last 900 orders per district)
--   order_line:  W * 300,000 rows (avg 10 lines per order)
--   item:        100,000 rows     (fixed, scale-independent)
--   stock:       W * 100,000 rows

CREATE TABLE warehouse (
    w_id       INTEGER        NOT NULL,
    w_name     VARCHAR(10)    NOT NULL,
    w_street_1 VARCHAR(20)    NOT NULL,
    w_street_2 VARCHAR(20)    NOT NULL,
    w_city     VARCHAR(20)    NOT NULL,
    w_state    VARCHAR(2)     NOT NULL,
    w_zip      VARCHAR(9)     NOT NULL,
    w_tax      DECIMAL(4,4)   NOT NULL,
    w_ytd      DECIMAL(12,2)  NOT NULL,
    PRIMARY KEY (w_id)
);

CREATE TABLE district (
    d_id        INTEGER        NOT NULL,
    d_w_id      INTEGER        NOT NULL,
    d_name      VARCHAR(10)    NOT NULL,
    d_street_1  VARCHAR(20)    NOT NULL,
    d_street_2  VARCHAR(20)    NOT NULL,
    d_city      VARCHAR(20)    NOT NULL,
    d_state     VARCHAR(2)     NOT NULL,
    d_zip       VARCHAR(9)     NOT NULL,
    d_tax       DECIMAL(4,4)   NOT NULL,
    d_ytd       DECIMAL(12,2)  NOT NULL,
    d_next_o_id INTEGER        NOT NULL,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer (
    c_id           INTEGER        NOT NULL,
    c_d_id         INTEGER        NOT NULL,
    c_w_id         INTEGER        NOT NULL,
    c_first        VARCHAR(16)    NOT NULL,
    c_middle       VARCHAR(2)     NOT NULL,
    c_last         VARCHAR(16)    NOT NULL,
    c_street_1     VARCHAR(20)    NOT NULL,
    c_street_2     VARCHAR(20)    NOT NULL,
    c_city         VARCHAR(20)    NOT NULL,
    c_state        VARCHAR(2)     NOT NULL,
    c_zip          VARCHAR(9)     NOT NULL,
    c_phone        VARCHAR(16)    NOT NULL,
    c_since        DATE           NOT NULL,
    c_credit       VARCHAR(2)     NOT NULL,
    c_credit_lim   DECIMAL(12,2)  NOT NULL,
    c_discount     DECIMAL(4,4)   NOT NULL,
    c_balance      DECIMAL(12,2)  NOT NULL,
    c_ytd_payment  DECIMAL(12,2)  NOT NULL,
    c_payment_cnt  INTEGER        NOT NULL,
    c_delivery_cnt INTEGER        NOT NULL,
    c_data         VARCHAR(500)   NOT NULL,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE hist (
    h_c_id   INTEGER        NOT NULL,
    h_c_d_id INTEGER        NOT NULL,
    h_c_w_id INTEGER        NOT NULL,
    h_d_id   INTEGER        NOT NULL,
    h_w_id   INTEGER        NOT NULL,
    h_date   DATE           NOT NULL,
    h_amount DECIMAL(6,2)   NOT NULL,
    h_data   VARCHAR(24)    NOT NULL
);

CREATE TABLE orders (
    o_id         INTEGER  NOT NULL,
    o_d_id       INTEGER  NOT NULL,
    o_w_id       INTEGER  NOT NULL,
    o_c_id       INTEGER  NOT NULL,
    o_entry_d    DATE     NOT NULL,
    o_carrier_id INTEGER,
    o_ol_cnt     INTEGER  NOT NULL,
    o_all_local  INTEGER  NOT NULL,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

CREATE TABLE new_order (
    no_o_id INTEGER NOT NULL,
    no_d_id INTEGER NOT NULL,
    no_w_id INTEGER NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE order_line (
    ol_o_id        INTEGER        NOT NULL,
    ol_d_id        INTEGER        NOT NULL,
    ol_w_id        INTEGER        NOT NULL,
    ol_number      INTEGER        NOT NULL,
    ol_i_id        INTEGER        NOT NULL,
    ol_supply_w_id INTEGER        NOT NULL,
    ol_delivery_d  DATE,
    ol_quantity    INTEGER        NOT NULL,
    ol_amount      DECIMAL(6,2)   NOT NULL,
    ol_dist_info   VARCHAR(24)    NOT NULL,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE TABLE item (
    i_id    INTEGER        NOT NULL,
    i_im_id INTEGER        NOT NULL,
    i_name  VARCHAR(24)    NOT NULL,
    i_price DECIMAL(5,2)   NOT NULL,
    i_data  VARCHAR(50)    NOT NULL,
    PRIMARY KEY (i_id)
);

CREATE TABLE stock (
    s_i_id       INTEGER     NOT NULL,
    s_w_id       INTEGER     NOT NULL,
    s_quantity   INTEGER     NOT NULL,
    s_dist_01    VARCHAR(24) NOT NULL,
    s_dist_02    VARCHAR(24) NOT NULL,
    s_dist_03    VARCHAR(24) NOT NULL,
    s_dist_04    VARCHAR(24) NOT NULL,
    s_dist_05    VARCHAR(24) NOT NULL,
    s_dist_06    VARCHAR(24) NOT NULL,
    s_dist_07    VARCHAR(24) NOT NULL,
    s_dist_08    VARCHAR(24) NOT NULL,
    s_dist_09    VARCHAR(24) NOT NULL,
    s_dist_10    VARCHAR(24) NOT NULL,
    s_ytd        INTEGER     NOT NULL,
    s_order_cnt  INTEGER     NOT NULL,
    s_remote_cnt INTEGER     NOT NULL,
    s_data       VARCHAR(50) NOT NULL,
    PRIMARY KEY (s_w_id, s_i_id)
);
