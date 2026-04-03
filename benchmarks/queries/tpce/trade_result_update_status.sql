-- name: Trade Result — Mark trades as completed
-- description: Write portion of Trade Result: transition pending trades to completed status
-- timeout: 30s

-- After settlement computation, the Trade Result transaction marks pending
-- trades as completed by updating their status from 'PNDG' to 'CMPT'.
UPDATE trade
SET t_st_id = 'CMPT',
    t_dts = CURRENT_TIMESTAMP
WHERE t_st_id = 'PNDG'
  AND t_id IN (
      SELECT t_id
      FROM trade
      WHERE t_st_id = 'PNDG'
      ORDER BY t_dts
      LIMIT 50
  );
