-- name: Catalog returns with high call center loss for specific demographics
-- timeout: 60s
SELECT cc_call_center_id AS call_center,
       cc_name,
       cc_manager,
       SUM(cr_net_loss) AS returns_loss
FROM catalog_returns, call_center, customer, customer_address,
     customer_demographics, date_dim, household_demographics
WHERE cr_call_center_sk        = cc_call_center_sk
  AND cr_returned_date_sk      = d_date_sk
  AND cr_returning_customer_sk = c_customer_sk
  AND cd_demo_sk               = c_current_cdemo_sk
  AND hd_demo_sk               = c_current_hdemo_sk
  AND ca_address_sk            = c_current_addr_sk
  AND d_year                   = 1998
  AND d_moy                    = 11
  AND (cd_marital_status       = 'M'
       AND cd_education_status = 'Unknown')
  AND hd_buy_potential         LIKE '>10000%'
  AND ca_gmt_offset            = -7
GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status,
         cd_education_status
ORDER BY returns_loss DESC
LIMIT 100;
