//Počet transakcií podľa blockchainu
SELECT 
    c.coin_name,
    COUNT(*) AS transaction_count
FROM FACT_TRANSACTIONS ft
JOIN DIM_COIN c ON ft.id_coin = c.id_coin
GROUP BY c.coin_name
ORDER BY transaction_count DESC;

//Počet transakcií v každom bloku
SELECT 
    b."blockNumber",
    COUNT(*) AS tx_in_block,
    ROUND(SUM(ft."fee"), 2) AS total_fees_in_block,
    ROUND(AVG(ft."fee"), 2) AS avg_fee_in_block
FROM FACT_TRANSACTIONS ft
JOIN DIM_BLOCK b ON ft.id_block = b.id_block
GROUP BY b."blockNumber"
ORDER BY tx_in_block DESC
LIMIT 20;

//Distribúcia hodnôt transakcií
SELECT 
    CASE 
        WHEN ft."value" = 0 THEN '0'
        WHEN ft."value" > 0 AND ft."value" <= 1000000 THEN '1 - 1M'
        WHEN ft."value" > 1000000 AND ft."value" <= 10000000 THEN '1M - 10M'
        WHEN ft."value" > 10000000 AND ft."value" <= 100000000 THEN '10M - 100M'
        ELSE '100M+'
    END AS value_range,
    COUNT(*) AS tx_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM FACT_TRANSACTIONS ft
GROUP BY value_range
ORDER BY 
    CASE value_range
        WHEN '0' THEN 1
        WHEN '1 - 1M' THEN 2
        WHEN '1M - 10M' THEN 3
        WHEN '10M - 100M' THEN 4
        ELSE 5
    END;

//Počet Fee vs Počet Value
SELECT 
    c.coin_name,
    ft."value",
    ft."fee",
    ft."status",
    COUNT(*) AS tx_count
FROM FACT_TRANSACTIONS ft
JOIN DIM_COIN c ON ft.id_coin = c.id_coin
GROUP BY c.coin_name, ft."value", ft."fee", ft."status"
ORDER BY ft."value" DESC, ft."fee" DESC;

//Priemerná hodnota transakcie s kontraktom
SELECT 
    c.coin_name,
    CASE 
        WHEN ac.id_address IS NOT NULL THEN 'With Contract'
        ELSE 'No Contract'
    END AS contract_type,
    ROUND(AVG(ft."value"), 2) AS avg_value,
FROM FACT_TRANSACTIONS ft
JOIN DIM_COIN c ON ft.id_coin = c.id_coin
LEFT JOIN DIM_ADDRESS ac ON ft.id_contract_address = ac.id_address
GROUP BY c.coin_name, contract_type
ORDER BY c.coin_name, contract_type;

