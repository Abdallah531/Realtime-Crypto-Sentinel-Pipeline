-- public.historical_prices definition

-- Drop table

-- DROP TABLE public.historical_prices;

CREATE TABLE public.historical_prices (
	"timestamp" timestamp NULL,
	coin text NULL,
	price float8 NULL
);


----------------------------------------------------
----------------------------------------------------

-- public.current_prices definition

-- Drop table

-- DROP TABLE public.current_prices;

CREATE TABLE public.current_prices (
	"timestamp" timestamp NULL,
	coin text NULL,
	price float8 NULL,
	market_cap int8 NULL
);