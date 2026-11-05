### Fixed

- TPCBB click and review item keys stay inside the TPC-DS item table above SF1 (#785)

    `web_clickstreams` and `product_reviews` drew item keys from `1..=scale * 18,000`, but the TPC-DS `item` table grows sublinearly above SF1 (102,000 rows at SF10, 204,000 at SF100). Above SF1 most keys had no item row: about 43% at SF10 and 89% at SF100. The item joins in q01, q03 and q04 silently lost those rows. Both generators now use the item table's row count. Data at SF1 and below is unchanged. SF10 and larger TPCBB datasets must be regenerated, and their q01, q03 and q04 now match about 1.8 times as many click and review rows at SF10 and 9 times as many at SF100, so those queries get slower than in earlier results.
