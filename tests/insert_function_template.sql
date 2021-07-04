CREATE OR REPLACE FUNCTION nft.insert_superrare(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH all_trades AS ( --https://etherscan.io/tx/0xa2c07597bb4350d1084e78c62059d30f2179d58eade0cb0612da4ffb0251d2bb
    SELECT
        tx_hash,
        block_time,
        CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
        CAST(substring(topic3 FROM 13) as TEXT) as "from",
        CAST(substring(topic4 FROM 13) as TEXT) as "to",
        bytea2numericpy(substring(data FOR 32)) original_amount_raw,
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
    SELECT   -- https://etherscan.io/tx/0x1acb61634e16bbbc94524dcc523ccd15137e6fd97a28993126354dce146cd310
        tx_hash,
        block_time,
        CAST(bytea2numericpy(topic4) as TEXT) AS token_id,
        CAST(substring(topic2 FROM 13) as TEXT) as "from",
        CAST(substring(topic3 FROM 13) as TEXT) as "to",
        bytea2numericpy(substring(data FOR 32)) original_amount_raw,
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
    SELECT   -- https://etherscan.io/tx/0x0ea7893c43530ab7e5946a17236fc53730e3d11e10c9922e05ac2d1b15bebf92
        tx_hash,
        block_time,
        CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
        CAST(substring(topic3 FROM 13) as TEXT) as "from",
        CAST(substring(topic4 FROM 13) as TEXT) as "to",
        bytea2numericpy(substring(data FOR 32)) original_amount_raw,
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
        CAST(substring(topic3 FROM 13) as TEXT) as "from",
        CAST(substring(topic4 FROM 13) as TEXT) as "to",
        bytea2numericpy(substring(data FOR 32)) original_amount_raw,
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
    SELECT   -- https://etherscan.io/tx/0x548d6a9d3b64e8012578435fc84f4b1f18e8ab2759a5d4a1d8d5fdbfc5b4e828
        tx_hash,
        block_time,
        CAST(bytea2numericpy(topic4) as TEXT) AS token_id,
        CAST(substring(topic2 FROM 13) as TEXT) as "from",
        CAST(substring(topic3 FROM 13) as TEXT) as "to",
        bytea2numericpy(substring(data FOR 32)) original_amount_raw,
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
    SELECT    -- https://etherscan.io/tx/0xeae8d230cd5ce305f6af3f7a5ce00586560092124c80a8c90f086ac9fc6c343c
        tx_hash,
        block_time,
        CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
        CAST(substring(topic3 FROM 13) as TEXT) as "from",
        CAST(substring(topic4 FROM 13) as TEXT) as "to",
        bytea2numericpy(substring(data FOR 32)) original_amount_raw,
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
        CAST(substring(topic3 FROM 13) as TEXT) as "from",
        CAST(substring(data FROM 13 FOR 20) as TEXT) as "to",
        bytea2numericpy(substring(data FROM 33 FOR 32)) original_amount_raw,
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
rows AS (
    INSERT INTO nft.trades (
	block_time,
	nft_project_name,
	nft_token_id,
	platform,
	platform_version,
	category,
	evt_type,
	usd_amount,
	seller,
	buyer,
	original_amount,
	original_amount_raw,
	original_currency,
	original_currency_contract,
	currency_contract,
	nft_contract_address,
	exchange_contract_address,
	tx_hash,
	block_number,
	tx_from,
	tx_to,
	trace_address,
	evt_index,
	trade_id
    )
    SELECT
	trades.block_time,
	labels.get(COALESCE(erc721.contract_address, erc20.contract_address), 'owner', 'project') AS nft_project_name, -- :todo: nft.name
	trades.token_id AS nft_token_id,
	'SuperRare' AS platform,
	trades.platform_version,
	category,
	'Trade' as evt_type,
	trades.original_amount_raw / 10^18 * prices.price AS usd_amount,
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
	trades.evt_index
        row_number() OVER (PARTITION BY platform, tx_hash, evt_index, category) AS trade_id -- :todo: :peer-review:
    FROM
	all_trades trades
    INNER JOIN ethereum.transactions tx
        ON trades.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN prices.usd prices ON prices.minute = date_trunc('minute', trades.block_time)
        AND prices.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND prices.minute >= start_ts
        AND prices.minute < end_ts
    LEFT JOIN erc721."ERC721_evt_Transfer" erc721 ON trades.tx_hash = erc721.evt_tx_hash
    LEFT JOIN erc20."ERC20_evt_Transfer" erc20 ON trades.tx_hash = erc20.evt_tx_hash
    WHERE category IN ('Buy','Offer Accepted','Auction Settled')
    AND trades.block_time >= start_ts
    AND trades.block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2019
SELECT nft.insert_superrare(
    '2019-01-01',
    '2020-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2019-01-01'
    AND block_time <= '2020-01-01'
    AND platform = 'SuperRare'
);


-- fill 2020
SELECT nft.insert_superrare(
    '2020-01-01',
    '2021-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2020-01-01'
    AND block_time <= '2021-01-01'
    AND platform = 'SuperRare'
);

-- fill 2021
SELECT nft.insert_superrare(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND platform = 'SuperRare'
);

INSERT INTO cron.job (schedule, command)
VALUES ('27 * * * *', $$
    SELECT nft.insert_superrare(
        (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='SuperRare'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='SuperRare')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
