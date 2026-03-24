-- SSB (Star Schema Benchmark) Schema DDL
-- Table names are unqualified; the benchmark runner prepends the namespace.
-- The date dimension is named dim_date (not date) to avoid conflict with the
-- SQL reserved keyword DATE.

CREATE TABLE dim_date (
    d_datekey          INTEGER      NOT NULL,
    d_date             VARCHAR(19)  NOT NULL,
    d_dayofweek        VARCHAR(9)   NOT NULL,
    d_month            VARCHAR(9)   NOT NULL,
    d_year             INTEGER      NOT NULL,
    d_yearmonthnum     INTEGER      NOT NULL,
    d_yearmonth        VARCHAR(8)   NOT NULL,
    d_daynuminweek     INTEGER      NOT NULL,
    d_daynuminmonth    INTEGER      NOT NULL,
    d_daynuminyear     INTEGER      NOT NULL,
    d_monthnuminyear   INTEGER      NOT NULL,
    d_weeknuminyear    INTEGER      NOT NULL,
    d_sellingseason    VARCHAR(12)  NOT NULL,
    d_lastdayinweekfl  INTEGER      NOT NULL,
    d_lastdayinmonthfl INTEGER      NOT NULL,
    d_holidayfl        INTEGER      NOT NULL,
    d_weekdayfl        INTEGER      NOT NULL
);

CREATE TABLE customer (
    c_custkey    INTEGER      NOT NULL,
    c_name       VARCHAR(25)  NOT NULL,
    c_address    VARCHAR(25)  NOT NULL,
    c_city       VARCHAR(10)  NOT NULL,
    c_nation     VARCHAR(15)  NOT NULL,
    c_region     VARCHAR(12)  NOT NULL,
    c_phone      VARCHAR(15)  NOT NULL,
    c_mktsegment VARCHAR(10)  NOT NULL
);

CREATE TABLE supplier (
    s_suppkey  INTEGER      NOT NULL,
    s_name     VARCHAR(25)  NOT NULL,
    s_address  VARCHAR(25)  NOT NULL,
    s_city     VARCHAR(10)  NOT NULL,
    s_nation   VARCHAR(15)  NOT NULL,
    s_region   VARCHAR(12)  NOT NULL,
    s_phone    VARCHAR(15)  NOT NULL
);

CREATE TABLE part (
    p_partkey  INTEGER      NOT NULL,
    p_name     VARCHAR(22)  NOT NULL,
    p_mfgr     VARCHAR(6)   NOT NULL,
    p_category VARCHAR(7)   NOT NULL,
    p_brand    VARCHAR(9)   NOT NULL,
    p_color    VARCHAR(11)  NOT NULL,
    p_type     VARCHAR(25)  NOT NULL,
    p_size     INTEGER      NOT NULL,
    p_container VARCHAR(10) NOT NULL
);

CREATE TABLE lineorder (
    lo_orderkey       BIGINT        NOT NULL,
    lo_linenumber     INTEGER       NOT NULL,
    lo_custkey        INTEGER       NOT NULL,
    lo_partkey        INTEGER       NOT NULL,
    lo_suppkey        INTEGER       NOT NULL,
    lo_orderdate      INTEGER       NOT NULL,
    lo_orderpriority  VARCHAR(15)   NOT NULL,
    lo_shippriority   VARCHAR(1)    NOT NULL,
    lo_quantity       INTEGER       NOT NULL,
    lo_extendedprice  BIGINT        NOT NULL,
    lo_ordertotalprice BIGINT       NOT NULL,
    lo_discount       INTEGER       NOT NULL,
    lo_revenue        BIGINT        NOT NULL,
    lo_supplycost     BIGINT        NOT NULL,
    lo_tax            INTEGER       NOT NULL,
    lo_commitdate     INTEGER       NOT NULL,
    lo_shipmode       VARCHAR(10)   NOT NULL
);
