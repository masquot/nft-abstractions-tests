WITH all_data AS (
-- Get data from various Rarible contracts deployed over time
-- Oct 2019 fading out in Summer 2020
    SELECT
        'Rarible' as platform,
        '1' as platform_version,
        '\xf2ee97405593bc7b6275682b0331169a48fedec7' AS exchange_contract_address,
        'direct_sale' as evt_type,
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token AS nft_contract_address,
        "tokenId" AS nft_token_id,
        seller,
        buyer,
        price AS original_amount_raw, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea as currency_contract,
        'TokenSale_evt_Buy' as category
     FROM rarible."TokenSale_evt_Buy" 
UNION ALL
-- from May 2020 to Sep 2020
    SELECT 
        'Rarible',
        '1',
        '\x8c530a698b6e83d562db09079bc458d4dad4e6c5',
        'direct_sale',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token,
        "tokenId",
        owner,
        buyer,
        price*value, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea,
        'ERC1155Sale_v1_evt_Buy'
    FROM rarible_v1."ERC1155Sale_v1_evt_Buy"
UNION ALL
-- from May 2020 to Sep 2020
    SELECT
        'Rarible',
        '1',
        '\xa5af48b105ddf2fa73cbaac61d420ea31b3c2a07',
        'direct_sale',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token,
        "tokenId",
        seller,
        buyer,
        price, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea,
        'ERC721Sale_v1_evt_Buy'
    FROM rarible_v1."ERC721Sale_v1_evt_Buy"
UNION ALL
-- from Sep 2020 and fading around end of 2020
    SELECT
        'Rarible',
        '1',
        '\x131aebbfe55bca0c9eaad4ea24d386c5c082dd58',
        'direct_sale',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token,
        "tokenId",
        seller,
        buyer,
        price, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea,
        'ERC721Sale_v2_evt_Buy'
    FROM rarible_v1."ERC721Sale_v2_evt_Buy"
UNION ALL
-- from Sep 2020 and fading around end of 2020
    SELECT
        'Rarible',
        '1',
        '\x93f2a75d771628856f37f256da95e99ea28aafbe',
        'direct_sale',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token,
        "tokenId",
        owner,
        buyer,
        price, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea,
        'ERC1155Sale_v2_evt_Buy'
    FROM rarible_v1."ERC1155Sale_v2_evt_Buy"
UNION ALL
-- from Nov 2020 ongoing
    SELECT
        'Rarible',
        '1',
        '\xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06',
        'direct_sale',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        "sellToken", -- :todo:
        "sellTokenId",
        owner,
        buyer,
        "buyValue" * amount / "sellValue", -- :todo:
        "buyToken", -- original_currency_contract 
        CASE
            WHEN "buyToken" = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE "buyToken"
        END, -- currency_contract,
        'ExchangeV1_evt_Buy' as category
    FROM rarible."ExchangeV1_evt_Buy"
    where "buyTokenId" = 0 --buy
UNION ALL
-- from Nov 2020 ongoing
    SELECT
        'Rarible',
        '1',
        '\xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06',
        'direct_sale',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        "buyToken",
        "buyTokenId",
        buyer AS seller,
        owner AS buyer,
        amount,
        "sellToken", -- original_currency_contract 
        CASE
            WHEN "sellToken" = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE "sellToken"
        END, -- currency_contract,
        'ExchangeV1_evt_Buy' as category
    FROM rarible."ExchangeV1_evt_Buy"
    where "sellTokenId" = 0 
)
SELECT
    a.evt_block_time,
    labels.get(a.nft_contract_address, 'owner', 'project') AS nft_project_name,
    a.nft_token_id,
    a.platform,
    a.platform_version,
    a.category,
    a.evt_type,
    a.original_amount_raw / 10 ^ erc20.decimals * p.price AS usd_amount,
    --    p.price AS currency_fx_rate,
    a.seller,
    a.buyer,
    a.original_amount_raw / 10 ^ erc20.decimals AS original_amount,
    a.original_amount_raw AS original_amount_raw,
    CASE 
    WHEN a.original_currency_contract = '\x0000000000000000000000000000000000000000'
    THEN 'ETH'
    ELSE erc20.symbol END AS original_currency,
    a.original_currency_contract,
    a.currency_contract, -- used for lookup
    a.nft_contract_address,
    a.exchange_contract_address,
    a.evt_tx_hash,
    a.evt_block_number,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    NULL::integer[] AS call_trace_address,
    a.evt_index
FROM
    all_data a
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = a.currency_contract
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', a.evt_block_time)
    AND p.contract_address = a.currency_contract
    LEFT JOIN ethereum.transactions tx ON tx.hash = a.evt_tx_hash
WHERE
    a.evt_tx_hash IN (
        '\x0b554efee155909455acdb4e4e1f08f81d5bda73babb8f514e80b35367a88309',
        '\xffea61d1b9b2a24e34daffef17d68fb196cc1211bd4822ceffaeda43feeca72c',
        '\x3ec34c0de96450026148a38d30a2b41e4b5541e8219a164ae8a8cd261d0a8c0c',
        '\x00028f7550d7bc9296e16a1dd08697b432514ec9698d54a8bcf233c12446bc93'
    )
LIMIT 100