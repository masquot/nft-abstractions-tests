--
-- \x90c80 OrdersMatched log data misleading
-- \x90c80 seller is https://etherscan.io/address/0xdf164e683920b376d1991a4149ad2f4155ac773b | addrs [9]
-- \x90c80 buyer is https://etherscan.io/address/0x7e2a67ecfce3ac2f55ebdd181b937350c9e1c78c | addrs [2], addrs [10]
-- \30c850 OrdersMatched log data correct 
-- https://opensea.io/assets/0x0e3a2a1f2146d86a604adc220b4967a898d7fe07/45660446
-- \30c850 seller is https://etherscan.io/address/0x8bbe743d57acb00e2f555c12fc5c752b069b9bee | addrs [3], addrs [9]
-- \30c850 buyer is https://etherscan.io/address/0xf983557ec70fbf1a4b1e247af7bf10247e9b69c4 | addrs [2]
--
-- `buyer` and 'seller' tested on all transactions in WHERE clause
SELECT
    call_tx_hash,
    -- 14 entries
    addrs [1] AS addr_1,
    addrs [2] AS addr_2,
    addrs [3] AS addr_3,
    addrs [4] AS addr_4,
    addrs [5] AS addr_5,
    addrs [6] AS addr_6,
    addrs [7] AS addr_7,
    addrs [8] AS addr_8,
    addrs [9] AS addr_9,
    addrs [10] AS addr_10,
    addrs [11] AS addr_11,
    addrs [12] AS addr_12,
    addrs [13] AS addr_13,
    addrs [14] AS addr_14,
    -- addrs [3] AS "seller",
    -- *,
    addrs [7] AS original_currency_address,
    CASE
        WHEN addrs [7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE addrs [7]
    END AS currency_token,
    addrs [5] AS contract_address,
    CAST(
        bytea2numericpy(
            substring(
                "calldataBuy"
                FROM
                    69 FOR 32
            )
        ) AS TEXT
    ) AS token_id
FROM
    opensea."WyvernExchange_call_atomicMatch_"
WHERE
    "call_success"
    AND call_tx_hash IN (
        '\x90c80aec81e25488aa86eea39c96e69ae0a7d6a4a63aaabe9f3f1a8a4239e18e',
        '\x30c850bc919390435f53980cbce75332b2c40b35dc21f8b74e07903a2abc181d',
        '\x805935fb7ecdd586e52fefa71770eb473f7fd5944a1777001b60c1f67a4a2b08',
        '\xdea0edbf7f4b2ec73db8810b29059b3df93180d58ec899446a67bb587efe6e60',
        '\x25ddf7c03a3bf193b13f633e93dd6d2a1a3df942b76a3aa19bc3887ff1125e56'
    )