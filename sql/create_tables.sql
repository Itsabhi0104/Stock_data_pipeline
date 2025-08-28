CREATE TABLE IF NOT EXISTS stock_price (
  symbol TEXT PRIMARY KEY,
  price DOUBLE PRECISION,
  change DOUBLE PRECISION,
  percent_change DOUBLE PRECISION,
  volume BIGINT,
  fetched_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stock_price_history (
  id SERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION,
  change DOUBLE PRECISION,
  percent_change DOUBLE PRECISION,
  volume BIGINT,
  fetched_at TIMESTAMPTZ DEFAULT now()
);