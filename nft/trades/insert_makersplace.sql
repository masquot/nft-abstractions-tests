CREATE OR REPLACE FUNCTION nft.insert_makersplace(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH all_data AS (
-- todo contract early transaction: 442 days 12 hrs ago (Apr-21-2020 08:43:10 PM +UTC) 
    SELECT
        'MakersPlace' as platform,
        '1' as platform_version,
        contract_address AS exchange_contract_address,
        'Trade' as evt_type,
        tx_hash AS evt_tx_hash,
        block_time AS evt_block_time,
        block_number AS evt_block_number,
        "index" AS evt_index,
        substring(data FROM 45 FOR 20) AS nft_contract_address,
        CAST(bytea2numericpy(substring(data FROM 1 FOR 32)) AS TEXT) AS nft_token_id,
        substring(data FROM 141 FOR 20) AS seller,
        substring(data FROM 109 FOR 20) AS buyer,
        bytea2numericpy(substring(data FROM 65 FOR 32)) original_amount_raw,
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract, -- :todo:
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea as currency_contract,
        'Buy' AS category -- 'Purchase'
    FROM ethereum."logs" 
    WHERE "contract_address" = '\x7e3abde9d9e80fa2d1a02c89e0eae91b233cde35' 
    AND topic1 = '\xdf5790120e3a47a9af8b7221574bf9c40e96cf1e648723b28b93a73ba9dd68fd'
    UNION ALL
    SELECT
        'MakersPlace' as platform,
        '1' as platform_version,
        contract_address AS exchange_contract_address,
        'Trade' as evt_type,
        tx_hash AS evt_tx_hash,
        block_time AS evt_block_time,
        block_number AS evt_block_number,
        "index" AS evt_index,
        substring(data FROM 45 FOR 20) AS nft_contract_address,
        CAST(bytea2numericpy(substring(data FROM 1 FOR 32)) AS TEXT) AS nft_token_id,
        substring(data FROM 141 FOR 20) AS seller, -- :todo:
        substring(data FROM 141 FOR 20) AS buyer,
        bytea2numericpy(substring(data FROM 65 FOR 32)) original_amount_raw,
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract, -- :todo:
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea as currency_contract,
        'Buy' AS category -- 'Purchase'
    FROM ethereum."logs" 
    WHERE "contract_address" = '\x7e3abde9d9e80fa2d1a02c89e0eae91b233cde35' 
    AND topic1 = '\xfc8d57c890a29ac7508080b26d7187224039062b525f377f0c7746193c59baa8'
 
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
        trades.evt_index
    FROM
        all_data trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = trades.currency_contract
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = trades.nft_contract_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = trades.currency_contract
        AND p.minute >= start_ts
        AND p.minute < end_ts
    WHERE
        trades.evt_block_time >= start_ts
        AND trades.evt_block_time < end_ts
   ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2019
SELECT nft.insert_makersplace(
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
    AND platform = 'MakersPlace'
);


-- fill 2020
SELECT nft.insert_makersplace(
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
    AND platform = 'MakersPlace'
);

-- fill 2021
SELECT nft.insert_makersplace(
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
    AND platform = 'MakersPlace'
);

INSERT INTO cron.job (schedule, command)
VALUES ('53 * * * *', $$
    SELECT nft.insert_makersplace(
        (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='MakersPlace'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='MakersPlace')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
