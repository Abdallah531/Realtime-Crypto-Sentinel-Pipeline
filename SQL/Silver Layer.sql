-- public.dim_crypto_prices definition

-- Drop table

-- DROP TABLE public.dim_crypto_prices;

CREATE TABLE public.dim_crypto_prices (
	"timestamp" timestamp NULL,
	coin text NULL,
	price float8 NULL,
	market_cap float8 NULL
);