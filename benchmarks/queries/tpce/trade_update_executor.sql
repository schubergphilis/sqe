-- name: Trade Update — Correct executor name
-- description: Write portion of Trade Update (Frame 1): update executor name on completed trades
-- timeout: 30s

-- The Trade Update transaction corrects executor names on completed trades.
-- TPC-E Frame 1 appends " X" to the executor name or removes it if already present.
UPDATE trade
SET t_exec_name = CASE
        WHEN t_exec_name LIKE '% X' THEN SUBSTRING(t_exec_name, 1, LENGTH(t_exec_name) - 2)
        ELSE t_exec_name || ' X'
    END
WHERE t_st_id = 'CMPT'
  AND t_id IN (
      SELECT t_id FROM trade
      WHERE t_st_id = 'CMPT'
      ORDER BY t_dts DESC
      LIMIT 20
  );
