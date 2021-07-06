WITH log_data AS (

-- 'Purchases' in ETH
SELECT
    'Rarible' as platform,
    '2' as platform_version,
    contract_address AS exchange_contract_address,
    'Trade' as evt_type,
    tx_hash AS evt_tx_hash,
    block_time AS evt_block_time,
    block_number AS evt_block_number,
    "index" AS evt_index,
    substring(data FROM 365 FOR 20) AS nft_contract_address,
    CAST(bytea2numericpy(substring(data FROM 385 FOR 32)) AS TEXT) AS nft_token_id,
    substring(data FROM 77 FOR 20) AS seller,
    substring(data FROM 109 FOR 20) AS buyer,
    bytea2numericpy(substring(data FROM 129 FOR 32)) original_amount_raw,
    bytea2numericpy(substring(data FROM 129 FOR 32)) / 10^18 AS original_amount, -- :todo: :remove: :for-testing:
    '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
    '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea as currency_contract,
    'Buy' AS category -- ? 'Purchase'
FROM ethereum."logs" 
WHERE "contract_address" = '\x9757f2d2b135150bbeb65308d4a91804107cd8d6' 
AND topic1 = '\x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4'
AND length(data) = 512
AND "tx_hash" IN (
    '\x8d0fcc7df9537146ccbe868503e6caa2d68ba9f0288304c28949e9c885458146', -- purchased ETH
    '\xa6b105513a5d9417857adc565413f329f0b25321fe0ef2347c377b2ea5bc106f'  -- purchased ETH
)
UNION ALL
-- 'Bid Accepted' non-ETH
SELECT
    'Rarible' as platform,
    '2' as platform_version,
    contract_address AS exchange_contract_address,
    'Trade' as evt_type,
     tx_hash AS evt_tx_hash,
    block_time AS evt_block_time,
    block_number AS evt_block_number,
    "index" AS evt_index,
    substring(data FROM 493 FOR 20) AS nft_contract_address, -- different !
    CAST(bytea2numericpy(substring(data FROM 513 FOR 32)) AS TEXT) AS nft_token_id, -- different !
    substring(data FROM 109 FOR 20) AS seller,
    substring(data FROM 77 FOR 20) AS buyer,
    bytea2numericpy(substring(data FROM 161 FOR 32)) AS original_amount_raw,
    bytea2numericpy(substring(data FROM 161 FOR 32)) / 10^18 AS original_amount, -- :todo: :remove: :for-testing:
    substring(data FROM 365 FOR 20) AS original_currency_contract,
    substring(data FROM 365 FOR 20) AS currency_contract,
    'Offer Accepted' AS category -- 'Bid Accepted'
FROM ethereum."logs"
WHERE "contract_address" = '\x9757f2d2b135150bbeb65308d4a91804107cd8d6'
AND topic1 = '\x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4'
AND length(data) = 544
AND bytea2numericpy(substring(data FROM 225 FOR 32)) = 384
AND "tx_hash" IN (
    '\xef0198728876e5375f25262bdad54f4c4a94f440f290ddf99ed3338ad1a98509', -- bid accepted RARI
    '\x0f782b0871c2fa3e65d58fdd5c0893c326db7d13e57063aac8deea117b780efc', -- bid accepted RARI
    '\xc858b5d64e38e7d47ddb312ef0494a7ba5a4ae9d7ddbfc1916ca665f316ab205', -- bid accepted WETH
    '\xacfb865cf02d299fe50cb7a15b62d15de0ef9bf43873ee8e84b8abb6d93e0c40'  -- bid accepted WETH
)
UNION ALL
-- 'Purchases' in non-ETH currencies
SELECT
    'Rarible' as platform,
    '2' as platform_version,
    contract_address AS exchange_contract_address,
    'Trade' as evt_type,
    tx_hash AS evt_tx_hash,
    block_time AS evt_block_time,
    block_number AS evt_block_number,
    "index" AS evt_index,
    substring(data FROM 365 FOR 20) AS nft_contract_address, -- different !
    CAST(bytea2numericpy(substring(data FROM 385 FOR 32)) AS TEXT) AS nft_token_id, -- different !
    substring(data FROM 77 FOR 20) AS seller,
    substring(data FROM 109 FOR 20) AS buyer,
    bytea2numericpy(substring(data FROM 129 FOR 32)) AS original_amount_raw,
    bytea2numericpy(substring(data FROM 129 FOR 32)) / 10^18 AS original_amount, -- :todo: :remove: :for-testing:
    substring(data FROM 525 FOR 20) AS original_currency_contract,
    substring(data FROM 525 FOR 20) AS currency_contract,
    'Buy' AS category -- 'Bid Accepted'
FROM ethereum."logs"
WHERE "contract_address" = '\x9757f2d2b135150bbeb65308d4a91804107cd8d6'
AND topic1 = '\x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4'
AND length(data) = 544
AND bytea2numericpy(substring(data FROM 225 FOR 32)) = 416
AND "tx_hash" IN (
    '\xba3443c5b8045a53e6c99f703a1e2d20beb8a3963cabaab1c97a010c9a1c09b9', -- purchase RARI
    '\xbc718a900de44ddb89c2117ff6d8a1a76169f8270233e74dee856d1468f5dfad'  -- purchase USDC
)


)
