SELECT
        call_tx_hash,
        CASE
            WHEN addrs [7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE addrs [7]
        END AS currency_token,
        addrs [5] AS contract_address,
        addrs [3] AS hex_token_id,
        CAST(bytea2numericpy(addrs [3]) as NUMERIC) AS token_id,
        *
    FROM
        opensea."WyvernExchange_call_atomicMatch_"
    WHERE
        "call_success"
    AND call_tx_hash = '\xdcf4809b4662c4a04709cee96f50515b13999810dd86dfa1a54371387e491f04'