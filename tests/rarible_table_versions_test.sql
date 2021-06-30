SELECT
    date_trunc('day', evt_block_time) AS DAY,
    SUM(price / 1e18) AS eth_volume
FROM
    (
        SELECT
            evt_block_time,
            evt_block_number,
            price
        FROM
            rarible_v1."ERC721Sale_v1_evt_Buy"
        UNION
        ALL
        SELECT
            evt_block_time,
            evt_block_number,
            price * value
        FROM
            rarible_v1."ERC1155Sale_v1_evt_Buy"
    ) AS eth_usd
GROUP BY
    DAY
ORDER BY
    DAY DESC;