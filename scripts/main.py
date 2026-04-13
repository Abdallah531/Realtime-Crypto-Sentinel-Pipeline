import requests
import pandas as pd
from datetime import datetime
import time
import os
from database_manger import load_to_db




def get_engine():
    from sqlalchemy import create_engine
    return create_engine("postgresql://myuser:mypassword@db:5432/crypto_db")


def historical_loader():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1
    }

    response = requests.get(url, params=params)
    coins_data = response.json()
    coin_ids = [coin["id"] for coin in coins_data]

    all_data = []
    print(coin_ids)

    for coin_id in coin_ids:
        print(f"Fetching historical data for {coin_id}...")
        hist_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        hist_params = {"vs_currency": "usd", "days": 7}
        
        hist_data = None 
        retries = 3

        for attempt in range(retries):
            try:
                hist_response = requests.get(hist_url, params=hist_params)
                hist_data = hist_response.json()

                if "prices" in hist_data:
                    break 
                else:
                    print(f"Retry {attempt+1} for {coin_id}...")
                    time.sleep(15)
            except Exception as e:
                print(f"Connection error for {coin_id}: {e}")
                time.sleep(15)

        if not hist_data or "prices" not in hist_data:
            print(f"❌ Failed completely for {coin_id}")
            continue

        for price_point in hist_data["prices"]:
            timestamp = datetime.fromtimestamp(price_point[0] / 1000)
            all_data.append({
                "timestamp": timestamp,
                "coin": coin_id,
                "price": price_point[1]
            })

        time.sleep(15)

    df = pd.DataFrame(all_data)
    load_to_db(df, "historical_prices") 
    print("Historical data loaded to Postgres ✅")

def current_loader():
    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1
    }

    response = requests.get(url, params=params)
    data = response.json()

    timestamp = datetime.now()

    rows = []

    for coin in data:
        rows.append({
            "timestamp": timestamp,
            "coin": coin["id"],
            "price": coin["current_price"],
            "market_cap": coin["market_cap"]
        })

    df = pd.DataFrame(rows)
    load_to_db(df, "current_prices")
    print("Current data updated in Postgres ✅")
 

def transform_crypto_data():   
    print("🔄 Starting Data Transformation...")
    engine = get_engine()
    
    try:
        # 1. Pull Data from Postgres
        hist_df = pd.read_sql("SELECT * FROM historical_prices", engine)
        curr_df = pd.read_sql("SELECT * FROM current_prices", engine)
        
        # 2. Combine & Clean
        combined_df = pd.concat([hist_df, curr_df], ignore_index=True)
        combined_df.drop_duplicates(subset=['timestamp', 'coin'], inplace=True)
        combined_df.sort_values(by=['coin', 'timestamp'], inplace=True)
        
        # 3. Save to Silver Table
        combined_df.to_sql("dim_crypto_prices", engine, if_exists='replace', index=False)
        
        print(f"✅ Transformation Complete! Saved {len(combined_df)} clean rows.")
    finally:
        engine.dispose() 


def run_full_pipeline():
    print("🚀 Starting Airflow Pipeline...")
    current_loader()
    print("✅ Pipeline Finished Successfully!")


def create_gold_correlation():
    engine = get_engine()
    df = pd.read_sql("SELECT timestamp, coin, price FROM dim_crypto_prices", engine)
    
    pivot_df = df.pivot_table(index='timestamp', columns='coin', values='price')
    
    corr_matrix = pivot_df.corr()
    
    corr_matrix.to_sql("gold_fact_correlation", engine, if_exists='replace')
    print("📈 Correlation Matrix created in 'gold_fact_correlation' table.")



def create_gold_signals():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM dim_crypto_prices ORDER BY coin, timestamp", engine)
    
    def compute_rsi(series, period=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    df['rsi'] = df.groupby('coin')['price'].transform(lambda x: compute_rsi(x))

    df['mean_24'] = df.groupby('coin')['price'].transform(lambda x: x.rolling(window=24).mean())
    df['std_24'] = df.groupby('coin')['price'].transform(lambda x: x.rolling(window=24).std())
    df['z_score'] = (df['price'] - df['mean_24']) / df['std_24']
    
    df['is_anomaly'] = df['z_score'].abs() > 2

    df.to_sql("gold_fact_signals", engine, if_exists='replace', index=False)
    print("🚨 Trading Signals (RSI & Anomalies) created in 'gold_fact_signals' table.")


def create_gold_daily_summary():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM gold_fact_signals", engine)
    df['date'] = pd.to_datetime(df['timestamp']).dt.date

    summary = df.groupby(['date', 'coin']).agg(
        avg_price=('price', 'mean'),
        max_price=('price', 'max'),
        min_price=('price', 'min'),
        avg_rsi=('rsi', 'mean'),
        total_anomalies=('is_anomaly', 'sum'),
        volatility_score=('price', 'std') 
    ).reset_index()

    summary.to_sql("gold_fact_daily_summary", engine, if_exists='replace', index=False)
    print("🏆 Daily Summary created in 'gold_fact_daily_summary' table.")


if __name__ == "__main__":
   
    # historical_loader() 
    
    run_full_pipeline()
    transform_crypto_data()

    create_gold_correlation()
    create_gold_signals()
    create_gold_daily_summary()
