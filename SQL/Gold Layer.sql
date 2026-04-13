-- public.gold_fact_correlation definition

-- Drop table

-- DROP TABLE public.gold_fact_correlation;

CREATE TABLE public.gold_fact_correlation (
	coin text NULL,
	binancecoin float8 NULL,
	bitcoin float8 NULL,
	dogecoin float8 NULL,
	ethereum float8 NULL,
	"figure-heloc" float8 NULL,
	ripple float8 NULL,
	solana float8 NULL,
	tether float8 NULL,
	tron float8 NULL,
	"usd-coin" float8 NULL
);
CREATE INDEX ix_gold_fact_correlation_coin ON public.gold_fact_correlation USING btree (coin);

------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------


-- public.gold_fact_daily_summary definition

-- Drop table

-- DROP TABLE public.gold_fact_daily_summary;

CREATE TABLE public.gold_fact_daily_summary (
	"date" date NULL,
	coin text NULL,
	avg_price float8 NULL,
	max_price float8 NULL,
	min_price float8 NULL,
	avg_rsi float8 NULL,
	total_anomalies int8 NULL,
	volatility_score float8 NULL
);


------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------


-- public.gold_fact_signals definition

-- Drop table

-- DROP TABLE public.gold_fact_signals;

CREATE TABLE public.gold_fact_signals (
	"timestamp" timestamp NULL,
	coin text NULL,
	price float8 NULL,
	market_cap float8 NULL,
	rsi float8 NULL,
	mean_24 float8 NULL,
	std_24 float8 NULL,
	z_score float8 NULL,
	is_anomaly bool NULL
);