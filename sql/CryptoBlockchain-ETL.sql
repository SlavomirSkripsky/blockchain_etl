use warehouse lemming_wh;
use database lemming_db;
create
or replace schema blockchain_etl;
use schema blockchain_etl;

// BTC
CREATE
OR REPLACE TABLE STG_BTC_PENDING AS
SELECT
    *
FROM
    BLOCKCHAIN_CRYPTO_DATA.AMBERDATA_BLOCKCHAIN.BITCOIN_PENDING_TRANSACTION;
    
// ETH
    CREATE
    OR REPLACE TABLE STG_ETH_PENDING AS
SELECT
    *
FROM
    BLOCKCHAIN_CRYPTO_DATA.AMBERDATA_BLOCKCHAIN.ETHEREUM_PENDING_TRANSACTION;
    
// LTC
    CREATE
    OR REPLACE TABLE STG_LTC_PENDING AS
SELECT
    *
FROM
    BLOCKCHAIN_CRYPTO_DATA.AMBERDATA_BLOCKCHAIN.LITECOIN_PENDING_TRANSACTION;

    
// overenie nahratia dát
SELECT
    *
FROM
    STG_BTC_PENDING;
SELECT
    *
FROM
    STG_ETH_PENDING;
SELECT
    *
FROM
    STG_LTC_PENDING;

    
// blockchainId cez coin_name, zjednotenie všetkych troch staging tabuliek
    CREATE
    OR REPLACE TABLE UNIFIED_STG AS
SELECT
    *,
    'BITCOIN' AS coin_name
FROM
    STG_BTC_PENDING
UNION ALL
SELECT
    *,
    'ETHEREUM' AS coin_name
FROM
    STG_ETH_PENDING
UNION ALL
SELECT
    *,
    'LITECOIN' AS coin_name
FROM
    STG_LTC_PENDING;
SELECT
    *
FROM
    UNIFIED_STG;
DESCRIBE TABLE UNIFIED_STG;


// dimenzie
    CREATE
    OR REPLACE TABLE DIM_COIN AS (
        SELECT
            DISTINCT coin_name,
            "blockchainId",
            CASE
                "blockchainId"
                WHEN '408fa195a34b533de9ad9889f076045e' THEN 'BITCOIN'
                WHEN '1c9c969065fcd1cf' THEN 'ETHEREUM'
                WHEN 'f94be61fd9f4fa684f992ddfd4e92272' THEN 'LITECOIN'
                ELSE 'UNKNOWN'
            END AS blockchain_id_check,
            HASH(coin_name, "blockchainId") AS id_coin
        FROM
            UNIFIED_STG
    );
select
    *
from
    dim_coin;
CREATE
    OR REPLACE TABLE DIM_BLOCK AS (
        SELECT
            DISTINCT "blockchainId",
            "blockHash",
            "blockNumber",
            HASH("blockchainId", "blockHash", "blockNumber") AS id_block
        FROM
            UNIFIED_STG
    );
select
    *
from
    dim_block;
CREATE
    OR REPLACE TABLE DIM_TRANSACTION_TYPE AS (
        SELECT
            DISTINCT "transactionTypeId",
            "type",
            "status",
            "isCoinbase",
            HASH(
                "transactionTypeId",
                "type",
                "status",
                "isCoinbase"
            ) AS id_transaction_type
        FROM
            UNIFIED_STG
    );
select
    *
from
    dim_transaction_type;
CREATE
    OR REPLACE TABLE DIM_ADDRESS AS (
        SELECT
            DISTINCT address,
            HASH(address) AS id_address
        FROM
            (
                SELECT
                    "from" AS address
                FROM
                    UNIFIED_STG
                UNION
                SELECT
                    "to"
                FROM
                    UNIFIED_STG
                UNION
                SELECT
                    "contractAddress"
                FROM
                    UNIFIED_STG
            )
    );
select
    *
from
    dim_address;
CREATE
    OR REPLACE TABLE DIM_TIME AS (
        SELECT
            DISTINCT "timestamp",
            "timestampNanoseconds",
            DATE(TO_TIMESTAMP("timestamp")) AS date,
            YEAR(TO_TIMESTAMP("timestamp")) AS year,
            MONTH(TO_TIMESTAMP("timestamp")) AS month,
            DAY(TO_TIMESTAMP("timestamp")) AS day,
            HOUR(TO_TIMESTAMP("timestamp")) AS hour,
            MINUTE(TO_TIMESTAMP("timestamp")) AS minute,
            SECOND(TO_TIMESTAMP("timestamp")) AS second,
            HASH("timestamp", "timestampNanoseconds") AS id_time
        FROM
            UNIFIED_STG
    );
select
    *
from
    dim_time;
CREATE
    OR REPLACE TABLE DIM_GAS AS (
        SELECT
            DISTINCT "gas",
            "gasPrice",
            "gasUsed",
            "cumulativeGasUsed",
            "maxFeePerGas",
            "maxPriorityFeePerGas",
            HASH(
                "gas",
                "gasPrice",
                "gasUsed",
                "cumulativeGasUsed",
                "maxFeePerGas",
                "maxPriorityFeePerGas"
            ) AS id_gas
        FROM
            UNIFIED_STG
    );
select
    *
from
    dim_gas;
CREATE
    OR REPLACE TABLE DIM_OP_PROTOCOL AS (
        SELECT
            DISTINCT "opProInputValue",
            "opProLockTime",
            "opProOutputValue",
            "opProSize",
            "opProVersion",
            "opProVirtualSize",
            "opProInputs",
            "opProOutputs",
            HASH(
                "opProInputValue",
                "opProLockTime",
                "opProOutputValue",
                "opProSize",
                "opProVersion",
                "opProVirtualSize",
                "opProInputs",
                "opProOutputs"
            ) AS id_op_protocol
        FROM
            UNIFIED_STG
    );
select
    *
from
    dim_op_protocol;

    
// tabuľka faktov
    CREATE
    OR REPLACE TABLE FACT_TRANSACTIONS AS (
        SELECT
            stg."hash" AS transaction_hash,
            c.id_coin,
            b.id_block,
            tt.id_transaction_type,
            ti.id_time,
            g.id_gas,
            op.id_op_protocol,
            af.id_address AS id_from_address,
            at.id_address AS id_to_address,
            ac.id_address AS id_contract_address,
            -- METRIKY
            stg."value",
            stg."fee",
            stg."nonce",
            stg."numLogs",
            stg."transactionIndex",
            stg."status",
            -- WINDOW FUNCTION #1
            ROW_NUMBER() OVER (
                PARTITION BY b.id_block
                ORDER BY
                    stg."transactionIndex"
            ) AS tx_order_in_block,
            SUM(stg."fee") OVER (
                PARTITION BY b.id_block
                ORDER BY
                    stg."transactionIndex" ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS cumulative_fee_in_block,
            -- TECHNICKÉ
            stg."input",
            stg."logsBloom",
            stg."r",
            stg."s",
            stg."v",
            stg."accessList"
        FROM
            UNIFIED_STG stg
            JOIN DIM_COIN c ON stg.coin_name = c.coin_name
            JOIN DIM_BLOCK b ON stg."blockHash" = b."blockHash"
            JOIN DIM_TRANSACTION_TYPE tt ON stg."transactionTypeId" = tt."transactionTypeId"
            AND stg."type" = tt."type"
            AND stg."status" = tt."status"
            JOIN DIM_TIME ti ON stg."timestamp" = ti."timestamp"
            LEFT JOIN DIM_GAS g ON stg."gas" = g."gas"
            LEFT JOIN DIM_OP_PROTOCOL op ON stg."opProSize" = op."opProSize"
            LEFT JOIN DIM_ADDRESS af ON stg."from" = af.address
            LEFT JOIN DIM_ADDRESS at ON stg."to" = at.address
            LEFT JOIN DIM_ADDRESS ac ON stg."contractAddress" = ac.address
    );
select
    *
from
    fact_transactions;

drop table STG_BTC_PENDING;
drop table STG_ETH_PENDING;
drop table STG_LTC_PENDING;
drop table unified_stg;