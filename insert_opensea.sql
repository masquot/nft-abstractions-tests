-- Tests of individual transactions: see https://github.com/masquot/nfts-research-dune/blob/main/projects/open-sea.md
WITH wyvern_calldata AS (
    SELECT
        call_tx_hash,
        addrs [5] AS contract_address,
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
    opensea.evt_block_time,
    labels.get(wc.contract_address, 'owner', 'project') AS nft_project_name,
    token_id AS nft_token_id,
    'OpenSea' AS platform,
    '1' AS platform_version,
    'buy' AS category,
    'direct_sale' AS evt_type,
    opensea.price / 10 ^ erc20.decimals * p.price AS usd_amount,
    --    p.price AS currency_fx_rate,
    wc.seller,
    wc.buyer,
    opensea.price / 10 ^ erc20.decimals AS original_amount,
    opensea.price AS original_amount_raw,
    erc20.symbol AS original_currency,
    currency_token AS original_currency_contract,
    wc.contract_address AS nft_contract_address,
    opensea.contract_address AS exchange_contract_address,
    opensea.evt_tx_hash,
    opensea.evt_block_number,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    call_trace_address,
    opensea."evt_index"
FROM
    opensea."WyvernExchange_evt_OrdersMatched" opensea
    LEFT JOIN wyvern_calldata wc ON wc.call_tx_hash = opensea.evt_tx_hash
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', opensea.evt_block_time)
    AND p.contract_address = wc.currency_token
    LEFT JOIN ethereum.transactions tx ON tx.hash = opensea.evt_tx_hash
WHERE
    wc.call_tx_hash IN (
        '\x90c80aec81e25488aa86eea39c96e69ae0a7d6a4a63aaabe9f3f1a8a4239e18e',
        '\x30c850bc919390435f53980cbce75332b2c40b35dc21f8b74e07903a2abc181d',
        '\x805935fb7ecdd586e52fefa71770eb473f7fd5944a1777001b60c1f67a4a2b08',
        '\xdea0edbf7f4b2ec73db8810b29059b3df93180d58ec899446a67bb587efe6e60',
        '\x25ddf7c03a3bf193b13f633e93dd6d2a1a3df942b76a3aa19bc3887ff1125e56'
    )