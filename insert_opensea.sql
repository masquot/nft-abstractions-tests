-- Tests of individual transactions: see https://github.com/masquot/nfts-research-dune/blob/main/projects/open-sea.md
--
-- :todo: Issue to be resolved later -> not all currencies supported by OpenSea are available in Dune `erc20.tokens` and `prices.usd` tables. See here:
-- https://duneanalytics.com/queries/66344
WITH wyvern_calldata AS (
    SELECT
        call_tx_hash,
        CASE
            WHEN addrs [7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE addrs [7]
        END AS currency_token,
        addrs [5] AS contract_address,
        CAST(
            bytea2numericpy(
                substring(
                    "calldataBuy"
                    FROM
                        69 FOR 32
                )
            ) AS NUMERIC
        ) AS token_id
    FROM
        opensea."WyvernExchange_call_atomicMatch_"
    WHERE
        "call_success"
)
SELECT
    -- 'opensea' AS project,
    -- '1' AS version,
    -- 'direct_sale' AS evt_type,
    -- 'buy' AS category
    opensea."evt_tx_hash",
    opensea."evt_block_time",
    -- "evt_block_number",
    wc.contract_address AS contract_address,
    labels.get(wc.contract_address, 'owner', 'project'),
    token_id,
    "maker" AS "from",
    "taker" AS "to",
    "price" / 10 ^ 18 AS price,
    -- :todo: units
    currency_token AS currency,
    erc20.symbol
FROM
    opensea."WyvernExchange_evt_OrdersMatched" opensea
    LEFT JOIN wyvern_calldata wc ON wc.call_tx_hash = opensea.evt_tx_hash
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
WHERE
    -- for testing
    opensea."evt_tx_hash" IN (
        '\x25ddf7c03a3bf193b13f633e93dd6d2a1a3df942b76a3aa19bc3887ff1125e56',
        '\xdcf4809b4662c4a04709cee96f50515b13999810dd86dfa1a54371387e491f04',
        '\x6b67140d550d574cba84772349a134d9055aa895bc8ad359e7754439f4c23894'
    )
ORDER BY
    opensea."evt_block_number" DESC
LIMIT
    1000