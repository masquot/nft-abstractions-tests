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
    wc.contract_address AS contract_address,
    labels.get(wc.contract_address, 'owner', 'project'),
    erc20.symbol,
    SUM("price" / 10 ^ 18) AS total_price
FROM
    opensea."WyvernExchange_evt_OrdersMatched" opensea
    LEFT JOIN wyvern_calldata wc ON wc.call_tx_hash = opensea.evt_tx_hash
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
WHERE -- for testing
    wc.contract_address IN ('\xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d','\x629a673a8242c2ac4b7b8c5d8735fbeac21a6205','\xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270')
GROUP BY 1,2,3