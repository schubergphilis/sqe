-- name: Net revenue from customers in specific zip code areas
-- timeout: 60s
WITH zips AS (
    SELECT DISTINCT ca_zip
    FROM (
        SELECT SUBSTR(ca_zip, 1, 5) AS ca_zip
        FROM customer_address
        WHERE SUBSTR(ca_zip, 1, 5) IN (
            '89436','30485','12345','76543','23141','88107',
            '34101','33? 97','24305','54933'
        )
        INTERSECT
        SELECT d_zip
        FROM date_dim d, store_sales s, customer_address ca
        WHERE d.d_qoy = 1 AND d.d_year = 1998
          AND s.ss_sold_date_sk = d.d_date_sk
          AND ca.ca_address_sk = s.ss_addr_sk
    ) x
)
SELECT s_store_name, SUM(ss_net_profit) AS net_profit
FROM store_sales, date_dim, store, customer_address
WHERE d_qoy = 1
  AND d_year = 1998
  AND ss_sold_date_sk = d_date_sk
  AND ss_store_sk = s_store_sk
  AND ss_addr_sk = ca_address_sk
  AND SUBSTR(ca_zip, 1, 5) IN (SELECT ca_zip FROM zips)
GROUP BY s_store_name
ORDER BY s_store_name
LIMIT 100;
