-- "tokenId" IS NULL: 350253 rows eg. tx hash 0xdcf4809b4662c4a04709cee96f50515b13999810dd86dfa1a54371387e491f04
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
    -- 'opensea' AS project,
    -- '1' AS version,
    -- 'direct_sale' AS evt_type,
    -- 'buy' AS category
    opensea."evt_tx_hash",
    opensea."evt_block_time",
    -- "evt_block_number",
    wc.contract_address AS contract_address,
    labels.get(wc.contract_address, 'owner', 'project'),
    "tokenId" AS token_id,
    "maker" AS "from",
    "taker" AS "to",
    "price" / 10 ^ 18 AS price,
    -- :todo: units
    currency_token AS currency,
    erc20.symbol -- unused fields: "evt_index", "evt_tx_hash", "metadata", "sellHash", "buyHash",
FROM
    -- :todo: be careful with INNER JOINS - do tests
    opensea."WyvernExchange_evt_OrdersMatched" opensea
    LEFT JOIN erc721."ERC721_evt_Transfer" erc721 ON erc721.evt_tx_hash = opensea.evt_tx_hash
    LEFT JOIN wyvern_calldata wc ON wc.call_tx_hash = opensea.evt_tx_hash
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
WHERE
    "tokenId" IS NULL