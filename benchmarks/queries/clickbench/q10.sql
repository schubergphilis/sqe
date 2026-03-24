-- name: ClickBench Q10 — Top mobile phone models
-- timeout: 30s
SELECT "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u
FROM hits
WHERE "MobilePhoneModel" <> ''
GROUP BY "MobilePhoneModel"
ORDER BY u DESC
LIMIT 10;
