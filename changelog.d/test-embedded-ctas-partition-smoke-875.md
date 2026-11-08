### Fixed

- CLI smoke test expects embedded CTAS to keep `PARTITIONED BY` (#875)

    `embedded_warehouse_ctas_refuses_partitioning_it_would_drop` still asserted the refusal
    that embedded CTAS on the cluster's CTAS handler removed, which failed `aikido-test-rust`
    on main and on every MR pipeline. It is now `embedded_warehouse_ctas_keeps_partitioning`
    and checks `SHOW CREATE TABLE` prints `PARTITIONED BY (id)`.
