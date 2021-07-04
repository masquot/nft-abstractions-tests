CREATE TABLE Superrare.superrare_full_activity_list5 (
    tx_hash bytea,
    block_time timestamptz NOT NULL,
    token_id TEXT,
    "from" TEXT,
    "to" TEXT,
    amount int,
    price int,
    category TEXT,
    gas_fee int,
    "include" TEXT
);

CREATE OR REPLACE FUNCTION Superrare.create_full_activity_list(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH all_activities AS ( --https://etherscan.io/tx/0xa2c07597bb4350d1084e78c62059d30f2179d58eade0cb0612da4ffb0251d2bb
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
        'Offer accepted' AS category
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
        'Offer accepted' AS category
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
        'Offer accepted' AS category
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
        CASE WHEN topic3 = '\x0000000000000000000000000000000000000000000000000000000000000000' THEN 'Auction retired' ELSE 'Auction settled' END AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x8c9f364bf7a56ed058fc63ef81c6cf09c833e656'
    AND
        topic1 = '\xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9'
),

token_burning as  
(
SELECT
CAST(bytea2numericpy(topic4) as TEXT) AS token_id,
'BURNED' AS "include"
    FROM 
        ethereum."logs"
    WHERE
        contract_address = '\xb932a70a57673d89f4acffbe830e8ed7f75fb9e0'
    AND
        topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND 
        topic3 = '\x00000000000000000000000041a322b28d0ff354040e2cbc676f0320d8c8850d'
    UNION ALL
    SELECT    -- https://etherscan.io/tx/0x3d8b5b7bf921c2608aa02c2743c07b39155d5156ff4971701aba76e5b1879452
        CAST(bytea2numericpy(data) as TEXT) AS token_id,
        'BURNED' AS "include"
    FROM 
        ethereum."logs"
    WHERE
        contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
    AND
        topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND 
        topic3 = '\x00000000000000000000000041a322b28d0ff354040e2cbc676f0320d8c8850d'
),

rows AS (
    INSERT INTO Superrare.superrare_full_activity_list5 (
    -- new
    a.evt_block_time,
    labels.get(a.nft_contract_address, 'owner', 'project') AS nft_project_name,
    a.nft_token_id,
    a.platform,
    a.platform_version,
    a.category,
    a.evt_type,
    a.original_amount_raw / 10 ^ erc20.decimals * p.price AS usd_amount,
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
    -- old
    -- tx_hash,
    -- block_time,
    -- token_id,
    -- "from",
    -- "to",
    -- amount,
    -- price,
    -- category,
    -- gas_fee,
    -- "include"
    )
    SELECT
    a.block_time,
    labels.get(COALESCE(erc721.contract_address, erc20.contract_address), 'owner', 'project') AS nft_project_name, -- :todo: nft.name
    a.token_id,
    'SuperRare',
    a.platform_version,
    category,
    'Trade', -- evt_type :todo:
    a.original_amount_raw / 10^18 * price,
    COALESCE(erc721."from", erc20."from"),
    COALESCE(erc721."to", erc20."to"),
    a.original_amount_raw / 10^18,
    a.original_amount_raw,
    'ETH',
    '\x0000000000000000000000000000000000000000'::bytea,
    '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea,
    COALESCE(erc721.contract_address, erc20.contract_address),
    a.exchange_contract_address,
    a.tx_hash,
    a.block_number,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    NULL::integer[] AS trace_address,
    a.evt_index
    FROM 
    all_activities a
    LEFT JOIN token_burning ON token_burning.token_id = a.token_id 
    LEFT JOIN (SELECT * FROM prices."layer1_usd" WHERE symbol = 'ETH' AND minute > '2019-01-01') price ON minute = date_trunc('minute', a.block_time)
    LEFT JOIN ethereum."transactions" tx ON tx.hash = a.tx_hash
    LEFT JOIN erc721."ERC721_evt_Transfer" erc721 ON a.tx_hash = erc721.evt_tx_hash
    LEFT JOIN erc20."ERC20_evt_Transfer" erc20 ON a.tx_hash = erc20.evt_tx_hash
    WHERE category IN ('Buy','Offer accepted','Auction settled')
    -- original WHERE symbol = 'ETH' --AND "include" IS NULL
    ORDER BY time DESC
    ON CONFLICT DO NOTHING
    RETURNING 1
)

SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


SELECT Superrare.create_full_activity_list('2021-03-14', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-03-14'), (SELECT MAX(number) FROM ethereum.blocks)) WHERE NOT EXISTS (SELECT * FROM Superrare.superrare_full_activity_list5 LIMIT 1)
INSERT INTO cron.job (schedule, command)
VALUES ('14 1 * * *', $$SELECT Superrare.create_full_activity_list((SELECT max(block_time) - interval '2 days' FROM Superrare.superrare_full_activity_list5), (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM Superrare.superrare_full_activity_list5)), (SELECT MAX(number) FROM ethereum.blocks));$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
