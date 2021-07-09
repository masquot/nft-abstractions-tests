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
),
all_data AS (
    -- Get data from various Rarible contracts deployed over time
    -- Oct 2019 fading out in Summer 2020
        SELECT
            'Rarible' as platform,
            '1' as platform_version,
            '\xf2ee97405593bc7b6275682b0331169a48fedec7'::bytea AS exchange_contract_address,
            'Trade' as evt_type,
            evt_tx_hash,
            evt_block_time,
            evt_block_number,
            evt_index,
            token AS nft_contract_address,
            CAST("tokenId" AS TEXT) AS nft_token_id,
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
            'Trade',
            evt_tx_hash,
            evt_block_time,
            evt_block_number,
            evt_index,
            token,
            CAST("tokenId" AS TEXT),
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
            'Trade',
            evt_tx_hash,
            evt_block_time,
            evt_block_number,
            evt_index,
            token,
            CAST("tokenId" AS TEXT),
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
            'Trade',
            evt_tx_hash,
            evt_block_time,
            evt_block_number,
            evt_index,
            token,
            CAST("tokenId" AS TEXT),
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
            'Trade',
            evt_tx_hash,
            evt_block_time,
            evt_block_number,
            evt_index,
            token,
            CAST("tokenId" AS TEXT),
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
            'Trade',
            evt_tx_hash,
            evt_block_time,
            evt_block_number,
            evt_index,
            "sellToken", -- :todo:
            CAST("sellTokenId" AS TEXT),
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
            'Trade',
            evt_tx_hash,
            evt_block_time,
            evt_block_number,
            evt_index,
            "buyToken",
            CAST( "buyTokenId" AS TEXT),
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
        UNION ALL
    -- from 2021-06-15 onwards
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
            '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea as currency_contract,
            'Buy' AS category -- 'Purchase'
        FROM ethereum."logs" 
        WHERE "contract_address" = '\x9757f2d2b135150bbeb65308d4a91804107cd8d6' 
        AND topic1 = '\x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4'
        AND length(data) = 512
    UNION ALL
    -- from 2021-06-15 onwards
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
            substring(data FROM 365 FOR 20) AS original_currency_contract,
            substring(data FROM 365 FOR 20) AS currency_contract,
            'Offer Accepted' AS category -- 'Bid Accepted'
        FROM ethereum."logs"
        WHERE "contract_address" = '\x9757f2d2b135150bbeb65308d4a91804107cd8d6'
        AND topic1 = '\x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4'
        AND length(data) = 544
        AND bytea2numericpy(substring(data FROM 225 FOR 32)) = 384
    UNION ALL
    -- from 2021-06-15 onwards
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
            substring(data FROM 525 FOR 20) AS original_currency_contract,
            substring(data FROM 525 FOR 20) AS currency_contract,
            'Buy' AS category -- 'Bid Accepted'
        FROM ethereum."logs"
        WHERE "contract_address" = '\x9757f2d2b135150bbeb65308d4a91804107cd8d6'
        AND topic1 = '\x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4'
        AND length(data) = 544
        AND bytea2numericpy(substring(data FROM 225 FOR 32)) = 416
    ),
all_trades AS (
        SELECT
            tx_hash,
            block_time,
            CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
            bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
            '2' as platform_version,
            contract_address AS exchange_contract_address,
            block_number,
            "index" AS evt_index,
            'Buy' AS category
        FROM
            ethereum."logs"
        WHERE
            contract_address = '\x2947f98c42597966a0ec25e92843c09ac17fbaa7'
        AND
            topic1 = '\x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9'
    UNION ALL
        SELECT
            tx_hash,
            block_time,
            CAST(bytea2numericpy(topic4) as TEXT) AS token_id,
            bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
            '1' as platform_version,
            contract_address,
            block_number,
            "index" AS evt_index,
            'Buy' AS category
        FROM
            ethereum."logs"
        WHERE
            contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
        AND
            topic1 = '\x16dd16959a056953a63cf14bf427881e762e54f03d86b864efea8238dd3b822f'
    UNION ALL
        SELECT
            tx_hash,
            block_time,
            CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
            bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
            '2' as platform_version,
            contract_address,
            block_number,
            "index" AS evt_index,
            'Buy' AS category
        FROM
            ethereum."logs"
        WHERE
            contract_address = '\x65b49f7aee40347f5a90b714be4ef086f3fe5e2c'
        AND
            topic1 = '\x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9'
    UNION ALL
        SELECT
            tx_hash,
            block_time,
            CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
            bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
            '2' as platform_version,
            contract_address,
            block_number,
            "index" AS evt_index,
            'Offer Accepted' AS category
        FROM
            ethereum."logs"
        WHERE
            contract_address = '\x2947f98c42597966a0ec25e92843c09ac17fbaa7'
        AND
            topic1 = '\x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6'
    UNION ALL
        SELECT
            tx_hash,
            block_time,
            CAST(bytea2numericpy(topic4) as TEXT) AS token_id,
            bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
            '1' as platform_version,
            contract_address,
            block_number,
            "index" AS evt_index,
            'Offer Accepted' AS category
        FROM
            ethereum."logs"
        WHERE
            contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
        AND
            topic1 = '\xd6deddb2e105b46d4644d24aac8c58493a0f107e7973b2fe8d8fa7931a2912be'
    UNION ALL
        SELECT
            tx_hash,
            block_time,
            CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
            bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
            '2' as platform_version,
            contract_address,
            block_number,
            "index" AS evt_index,
            'Offer Accepted' AS category
        FROM
            ethereum."logs"
        WHERE
            contract_address = '\x65b49f7aee40347f5a90b714be4ef086f3fe5e2c'
        AND
            topic1 = '\x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6'
    UNION ALL
        SELECT
            tx_hash,
            block_time,
            CAST(bytea2numericpy(topic4) as TEXT) AS token_id,
            bytea2numericpy(substring(data FROM 33 FOR 32)) AS original_amount_raw,
            '2' as platform_version,
            contract_address,
            block_number,
            "index" AS evt_index,
            CASE WHEN topic3 = '\x0000000000000000000000000000000000000000000000000000000000000000' THEN 'Auction Retired' ELSE 'Auction Settled' END AS category
        FROM
            ethereum."logs"
        WHERE
            contract_address = '\x8c9f364bf7a56ed058fc63ef81c6cf09c833e656'
        AND
            topic1 = '\xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9'
),
all_exchanges AS (
    --test-data-types
    SELECT
    (SELECT NOW())::timestamptz AS block_time,
    'test_nft_roject'::TEXT AS nft_project_name,
    '1'::TEXT AS nft_token_id,
    'test_platform'::TEXT AS platform,
    '1'::TEXT AS platform_version,
    'test_category'::TEXT AS category,
    'test_evt_type'::TEXT AS evt_type,
    1.5::NUMERIC AS usd_amount,
    '\x4d93c788b6e9771f1ee2f30242cd3892b631d8ed'::BYTEA AS seller,
    '\x4d93c788b6e9771f1ee2f30242cd3892b631d8ed'::BYTEA AS buyer,
    1.5::NUMERIC AS original_amount,
    1.5::NUMERIC AS original_amount_raw,
    'test_ETH'::TEXT AS original_currency,
    '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA AS original_currency_contract,
    '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA AS currency_contract,
    '\xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'::BYTEA AS nft_contract_address,
    '\xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'::BYTEA AS exchange_contract_address,
    '\x9f7d76df04627e689d4c5b88a0e9d3108e21f457577360c19f897532a9d65668'::BYTEA AS tx_hash,
    100::integer as block_number,
    '\x4d93c788b6e9771f1ee2f30242cd3892b631d8ed'::BYTEA AS tx_from,
    '\x4d93c788b6e9771f1ee2f30242cd3892b631d8ed'::BYTEA AS tx_to,
    NULL::integer[] AS trace_address,
    100::integer as evt_index,
    100::integer as trade_id
    UNION ALL
    --cryptopunks
    SELECT
        trades.evt_block_time AS block_time,
        'CryptoPunks' AS nft_project_name,
        CAST(trades."punkIndex" AS TEXT) AS nft_token_id,
        'LarvaLabs Contract' AS platform,
        '1' AS platform_version,
        'Buy' AS category,
        'Trade' AS evt_type,
        trades.value / 10 ^ 18 * p.price AS usd_amount,
        trades."fromAddress" AS seller,
        trades."toAddress" AS buyer,
        trades.value / 10 ^ 18 AS original_amount,
        trades.value AS original_amount_raw,
        'ETH' AS original_currency,
        '\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
        trades.contract_address AS nft_contract_address,
        trades.contract_address AS exchange_contract_address,
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number AS block_number,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL::integer[] AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY 4, 18, 23, 6) AS trade_id -- :todo: :peer-review: (PARTITION BY platform, tx_hash, evt_index, category)
    FROM
        cryptopunks."CryptoPunksMarket_evt_PunkBought" trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    WHERE date_trunc('day', trades.evt_block_time) = '2021-07-08'

    UNION ALL
    --foundation
    SELECT
        trades.evt_block_time AS block_time,
        tokens.name AS nft_project_name,
        CAST(created."tokenId" AS TEXT) AS nft_token_id,
        'Foundation' AS platform,
        '1' AS platform_version,
        'Auction Settled' AS category,
        'Trade' AS evt_type,
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18 * p.price AS usd_amount, --
        trades.seller, --
        trades.bidder AS buyer, --
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18 AS original_amount, --
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") AS original_amount_raw, --
        'ETH' AS original_currency,
        '\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
        created."nftContract" AS nft_contract_address, -- Foundation NFT
        trades.contract_address AS exchange_contract_address, -- Foundation: Market
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number AS block_number,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL::integer[] AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY 4, 18, 23, 6) AS trade_id -- :todo: :peer-review: (PARTITION BY platform, tx_hash, evt_index, category)
    FROM
        foundation."market_evt_ReserveAuctionFinalized" trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
    LEFT JOIN foundation."market_evt_ReserveAuctionCreated" created ON trades."auctionId" = created."auctionId"
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = created."nftContract"
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    WHERE date_trunc('day', trades.evt_block_time) = '2021-07-08'

    UNION ALL
    --opensea
    SELECT
        trades.evt_block_time AS block_time,
        tokens.name AS nft_project_name,
        token_id AS nft_token_id,
        'OpenSea' AS platform,
        '1' AS platform_version,
        'Buy' AS category,
        'Trade' AS evt_type,
        trades.price / 10 ^ erc20.decimals * p.price AS usd_amount,
        wc.seller,
        wc.buyer,
        trades.price / 10 ^ erc20.decimals AS original_amount,
        trades.price AS original_amount_raw,
        CASE WHEN wc.original_currency_address = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
        wc.original_currency_address AS original_currency_contract,
        wc.currency_token AS currency_contract,
        wc.nft_contract_address AS nft_contract_address,
        trades.contract_address AS exchange_contract_address,
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        call_trace_address AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY 4, 18, 23, 6) AS trade_id -- (PARTITION BY platform, tx_hash, evt_index, category)
    FROM
        opensea."WyvernExchange_evt_OrdersMatched" trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
    LEFT JOIN wyvern_calldata wc ON wc.call_tx_hash = trades.evt_tx_hash
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = wc.nft_contract_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = wc.currency_token
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
    LEFT JOIN erc721."ERC721_evt_Transfer" erc721 ON trades.evt_tx_hash = erc721.evt_tx_hash
    WHERE
        erc721."from" <> '\x0000000000000000000000000000000000000000' 
    AND date_trunc('day', trades.evt_block_time) = '2021-07-08'

    UNION ALL
    --rarible
    SELECT
        trades.evt_block_time AS block_time,
        tokens.name AS nft_project_name,
        trades.nft_token_id,
        trades.platform,
        trades.platform_version,
        trades.category,
        trades.evt_type, -- :todo:
        trades.original_amount_raw / 10 ^ erc20.decimals * p.price AS usd_amount,
        trades.seller,
        trades.buyer,
        trades.original_amount_raw / 10 ^ erc20.decimals AS original_amount,
        trades.original_amount_raw AS original_amount_raw,
        CASE WHEN trades.original_currency_contract = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
        trades.original_currency_contract,
        trades.currency_contract,
        trades.nft_contract_address,
        trades.exchange_contract_address,
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number AS block_number,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL::integer[] AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY 4, 18, 23, 6) AS trade_id -- (PARTITION BY platform, tx_hash, evt_index, category)
    FROM
        all_data trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = trades.currency_contract
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = trades.nft_contract_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = trades.currency_contract
    WHERE date_trunc('day', trades.evt_block_time) = '2021-07-08'

    UNION ALL
    --superrare

   
    SELECT
	trades.block_time,
        tokens.name AS nft_project_name,
	trades.token_id AS nft_token_id,
	'SuperRare' AS platform,
	trades.platform_version,
	category,
	'Trade' as evt_type,
	trades.original_amount_raw / 10^18 * p.price AS usd_amount,
	COALESCE(erc721."from", erc20."from") AS seller,
	COALESCE(erc721."to", erc20."to") AS buyer,
	trades.original_amount_raw / 10^18 as original_amount,
	trades.original_amount_raw as original_amount_raw,
	'ETH' AS original_currency,
	'\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract, 
	'\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
	COALESCE(erc721.contract_address, erc20.contract_address) AS nft_contract_address,
	trades.exchange_contract_address,
	trades.tx_hash,
	trades.block_number,
	tx."from" AS tx_from,
	tx."to" AS tx_to,
	NULL::integer[] AS trace_address,
	trades.evt_index,
	row_number() OVER (PARTITION BY 4, 18, 23, 6) AS trade_id -- :todo: :peer-review: (PARTITION BY platform, tx_hash, evt_index, category
    FROM
	all_trades trades
    INNER JOIN ethereum.transactions tx
        ON trades.tx_hash = tx.hash
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.block_time)
        AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    LEFT JOIN erc721."ERC721_evt_Transfer" erc721 ON trades.tx_hash = erc721.evt_tx_hash
    LEFT JOIN erc20."ERC20_evt_Transfer" erc20 ON trades.tx_hash = erc20.evt_tx_hash
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = COALESCE(erc721.contract_address, erc20.contract_address)
    WHERE category IN ('Buy','Offer Accepted','Auction Settled')
    AND date_trunc('day', trades.block_time) = '2021-07-08'
)
SELECT * FROM all_exchanges 
WHERE date_trunc('day', block_time) = '2021-07-08'

