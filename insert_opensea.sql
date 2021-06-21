-- first test: bored ape yacht club
-- contract: '\xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'
-- transaction for https://opensea.io/assets/0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d/3353
-- https://etherscan.io/tx/0x25ddf7c03a3bf193b13f633e93dd6d2a1a3df942b76a3aa19bc3887ff1125e56#eventlog
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
    INNER JOIN erc721."ERC721_evt_Transfer" erc721 ON erc721.evt_tx_hash = opensea.evt_tx_hash
    INNER JOIN wyvern_calldata wc ON wc.call_tx_hash = opensea.evt_tx_hash
    INNER JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
-- WHERE
--     erc20.symbol <> 'WETH' -- for testing
    --    taker = '\x9a8721a9d73ab3768b268e525455d6378cb3eb48'
ORDER BY
    opensea."evt_block_number" DESC
LIMIT
    1000