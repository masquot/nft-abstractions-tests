WITH wyvern_calldata AS (
    SELECT
        call_tx_hash,
        addrs [5] AS nft_contract_address,
        addrs [2] AS buyer,
        addrs [9] AS seller,
        addrs [7] AS original_currency_address,
        CASE
            WHEN addrs [7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE addrs [7]
        END AS currency_token,
        CAST(
            bytea2numericpy(
                substring(
                    "calldataBuy"
                    FROM
                        69 FOR 32
                )
            ) AS TEXT
        ) AS token_id,
        call_trace_address
    FROM
        opensea."WyvernExchange_call_atomicMatch_"
    WHERE
        "call_success"
)
SELECT
--    trades.evt_block_time AS block_time,
      seller,
      t_erc721."from",
      buyer,
      t_erc721."to",
--    trades.price / 10 ^ erc20.decimals * p.price AS usd_amount,
--      DISTINCT(wc.seller = COALESCE(t_erc721."from")),
--    wc.seller,
--    COALESCE(t_erc721."from") AS t_seller,
--    wc.buyer,
--    trades.price / 10 ^ erc20.decimals AS original_amount,
--    CASE WHEN wc.original_currency_address = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
--    COUNT(*) -- :todo: :peer-review:
FROM
    opensea."WyvernExchange_evt_OrdersMatched" trades
LEFT JOIN wyvern_calldata wc ON wc.call_tx_hash = trades.evt_tx_hash
LEFT JOIN erc721."ERC721_evt_Transfer" t_erc721 ON trades.evt_tx_hash = t_erc721.evt_tx_hash
--LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
WHERE
       token_id IN ('1', '2', '3')
--       AND t_erc721."from" IS NOT NULL

