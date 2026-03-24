-- name: ClickBench Q11 — Top mobile phone + model combos
-- timeout: 30s
SELECT "MobilePhone", "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u
FROM hits
WHERE "MobilePhoneModel" <> ''
GROUP BY "MobilePhone", "MobilePhoneModel"
ORDER BY u DESC
LIMIT 10;
