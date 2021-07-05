CREATE TABLE nft.trades (
    block_time timestamptz NOT NULL,
    nft_project_name text,
    nft_token_id text,
    platform text NOT NULL,
    platform_version text,
    category text,
    evt_type text,
    usd_amount numeric,
    seller bytea,
    buyer bytea,
    original_amount numeric,
    original_amount_raw numeric,
    original_currency text,
    original_currency_contract bytea,
    currency_contract bytea,
    nft_contract_address bytea NOT NULL,
    exchange_contract_address bytea NOT NULL,
    tx_hash bytea NOT NULL,
    block_number integer,
    tx_from bytea NOT NULL,
    tx_to bytea,
    trace_address integer[],
    evt_index integer,
    trade_id integer
);

-- :todo: :help: is this always unique
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS nft_trades_platform_tx_hash_evt_index_uniq_idx ON nft.trades (platform, tx_hash, evt_index);
CREATE INDEX IF NOT EXISTS nft_trades_block_time_idx ON nft.trades USING BRIN (block_time);
CREATE INDEX IF NOT EXISTS nft_trades_seller_idx ON nft.trades (seller);
CREATE INDEX IF NOT EXISTS nft_trades_buyer_idx ON nft.trades (buyer);
CREATE INDEX IF NOT EXISTS nft_trades_nft_project_name_nft_token_id_block_time_idx ON nft.trades (nft_project_name, nft_token_id, block_time)
-- overkill ?
CREATE INDEX IF NOT EXISTS nft_trades_block_time_platform_seller_buyer_nft_project_name_nft_token_id_idx ON nft.trades (block_time, platform, seller, buyer, nft_project_name, nft_token_id)

-- === original indexes ===
-- CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS nft_trades_proj_tr_addr_uniq_idx ON nft.trades (project, tx_hash, trace_address, trade_id);
-- CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS nft_trades_proj_evt_index_uniq_idx ON nft.trades (project, tx_hash, evt_index, trade_id);
-- CREATE INDEX IF NOT EXISTS nft_trades_tx_from_idx ON nft.trades (tx_from);
-- CREATE INDEX IF NOT EXISTS nft_trades_tx_to_idx ON nft.trades (tx_to);
-- CREATE INDEX IF NOT EXISTS nft_trades_project_idx ON nft.trades (project);
-- CREATE INDEX IF NOT EXISTS nft_trades_block_time_idx ON nft.trades USING BRIN (block_time);
-- CREATE INDEX IF NOT EXISTS nft_trades_token_a_idx ON nft.trades (token_a_address);
-- CREATE INDEX IF NOT EXISTS nft_trades_token_b_idx ON nft.trades (token_b_address);
-- CREATE INDEX IF NOT EXISTS nft_trades_block_time_project_idx ON nft.trades (block_time, project)
