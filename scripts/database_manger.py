import pandas as pd

def load_to_db(df, table_name):
    from sqlalchemy import create_engine 
    
    DB_URL = "postgresql://myuser:mypassword@db:5432/crypto_db"
    
    if df.empty:
        print("Empty DataFrame. Nothing to load.")
        return
    
    engine = create_engine(DB_URL)
    
    try:
        # Upload Data to database
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✅ Successfully loaded {len(df)} rows to table: {table_name}")
        
        engine.dispose() 
        
    except Exception as e:
        print(f"❌ Error loading to Postgres: {e}")