-- TPC-BB (BigBench) Additional Tables Schema DDL
-- This file contains only the two TPC-BB–specific tables.
-- The full TPC-DS base tables (store_sales, store_returns, item, customer,
-- date_dim, web_sales, catalog_sales, customer_demographics, customer_address,
-- web_page, etc.) are defined in tpcds.sql and must be created separately.
-- Table names are unqualified; the benchmark runner prepends the namespace.

CREATE TABLE web_clickstreams (
    wcs_click_date_sk    INTEGER,
    wcs_click_time_sk    INTEGER,
    wcs_sales_sk         BIGINT,
    wcs_item_sk          INTEGER,
    wcs_web_page_sk      INTEGER,
    wcs_user_sk          INTEGER,
    wcs_referrer_url     VARCHAR(100),
    wcs_search_keywords  VARCHAR(100)
);

CREATE TABLE product_reviews (
    pr_review_sk       BIGINT       NOT NULL,
    pr_review_date     DATE,
    pr_review_time     VARCHAR(8),
    pr_review_rating   INTEGER      NOT NULL,
    pr_item_sk         INTEGER,
    pr_user_sk         INTEGER,
    pr_order_sk        BIGINT,
    pr_review_content  VARCHAR(4000),
    pr_title           VARCHAR(100)
);
