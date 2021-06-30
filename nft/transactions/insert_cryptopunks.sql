(
-- event PunkBought
-- all purchases via main contract in ETH
SELECT
    c.evt_block_time,
    'CryptoPunks' AS nft_project_name,
    c."punkIndex" AS nft_token_id,
    'LarvaLabs Contract' AS platform,
    '1' AS platform_version,
    'buy' AS category,
    'direct_sale' AS evt_type,
    c.value / 10 ^ erc20.decimals * p.price AS usd_amount,
    c."fromAddress",
    c."toAddress",
    c.value / 10 ^ erc20.decimals AS original_amount,
    c.value AS original_amount_raw,
    'ETH' AS original_currency,
    '\x0000000000000000000000000000000000000000' AS original_currency_contract,
    c.contract_address AS nft_contract_address,
    c.contract_address AS exchange_contract_address,
    c.evt_tx_hash,
    c.evt_block_number,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    NULL::integer[] AS call_trace_address,
    c.evt_index
FROM
    cryptopunks."CryptoPunksMarket_evt_PunkBought" c -- use 'WETH' to look uo 'ETH' price
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', c.evt_block_time)
    AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    LEFT JOIN ethereum.transactions tx ON tx.hash = c.evt_tx_hash
)