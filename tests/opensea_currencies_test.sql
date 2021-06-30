-- https://support.opensea.io/hc/en-us/articles/1500003082521-What-currencies-can-I-use-on-OpenSea-
WITH wyvern_calldata AS (
    SELECT
        call_tx_hash,
        CASE
            WHEN addrs [7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE addrs [7]
        END AS currency_token,
        addrs [5] AS contract_address
    FROM
        opensea."WyvernExchange_call_atomicMatch_"
    WHERE
        "call_success"
)
SELECT
    currency_token AS currency,
    erc20.symbol,
    p.decimals,
    COUNT(*)
FROM
    opensea."WyvernExchange_evt_OrdersMatched" opensea
    LEFT JOIN wyvern_calldata wc ON wc.call_tx_hash = opensea.evt_tx_hash
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', opensea.evt_block_time)
           AND p.contract_address = wc.currency_token
GROUP BY 1, 2, 3
ORDER BY 4 DESC