import json
import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

env_path = Path(__file__).parents[1] / ".env"

load_dotenv(dotenv_path=env_path)


DB_CONFIG = {
    "host": os.getenv("WIN_POSTGRES_HOST"),
    "port": os.getenv("WIN_POSTGRES_PORT"),
    "user": os.getenv("WIN_POSTGRES_USER"),
    "password": os.getenv("WIN_POSTGRES_PASSWORD"),
    "database": os.getenv("WIN_POSTGRES_DB"),
}


def get_engine(config):
    encoded_pwd = quote_plus(config["password"])
    conn_str = f"postgresql://{config['user']}:{encoded_pwd}@{config['host']}:{config['port']}/{config['database']}"
    return create_engine(
        conn_str, pool_size=10, max_overflow=10, pool_timeout=30, pool_recycle=1800
    )


engine = get_engine(DB_CONFIG)


def query_db(sql):
    try:
        with engine.connect() as conn:
            return pd.read_sql(sql, conn)
    except Exception as e:
        print(f"[DB Error] Querying: {e}")
        return pd.DataFrame()

def push_df_to_db(df, table_name, schema="raw", primary_key="id"):
    if df is None or df.empty:
        print(f"  [Skip] DataFrame is empty for {schema}.{table_name}")
        return

    df_processed = df.copy()

    # 1. Xử lý Json cho các cột dict/list
    for col in df_processed.columns:
        if not df_processed[col].dropna().empty:
            sample_val = df_processed[col].dropna().iloc[0]
            if isinstance(sample_val, (dict, list)):
                df_processed[col] = df_processed[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
                )

    # 2. Tự động Migration (Thêm cột/Tạo bảng)
    try:
        with engine.begin() as conn:
            # Kiểm tra cột trong bảng cụ thể của schema
            existing_cols_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table
            """)
            
            result = conn.execute(existing_cols_query, {"schema": schema, "table": table_name})
            existing_cols = [row[0] for row in result.fetchall()]

            if not existing_cols:
                # Nếu bảng chưa có, dùng to_sql để tạo nhanh (với schema)
                df_processed.head(0).to_sql(
                    table_name, engine, schema=schema, if_exists="append", index=False
                )
                conn.execute(text(f'ALTER TABLE "{schema}"."{table_name}" ADD PRIMARY KEY ("{primary_key}");'))
            else:
                for col in df_processed.columns:
                    if col not in existing_cols:
                        print(f"  [Migration] Adding column {col} to {schema}.{table_name}")
                        conn.execute(text(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{col}" TEXT;'))
    except Exception as e:
        print(f"  [Migration Error] {e}")

    # 3. Logic UPSERT
    df_clean = df_processed.astype(object).where(pd.notnull(df_processed), None)
    data_dicts = df_clean.to_dict(orient='records')

    cols = df_clean.columns.tolist()
    col_names = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join([f":{c}" for c in cols])

    # ---- HANDLE MULTI PRIMARY KEY ----
    if isinstance(primary_key, (list, tuple)):
        conflict_cols = list(primary_key)
    else:
        conflict_cols = [primary_key]

    conflict_clause = ", ".join(conflict_cols)

    update_cols = ", ".join(
        [f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in conflict_cols]
    )

    upsert_sql = text(f"""
        INSERT INTO "{schema}"."{table_name}" ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {update_cols}
    """)

    try:
        with engine.begin() as conn:
            conn.execute(upsert_sql, data_dicts)
        print(f"  [DB Success] Upserted {len(df)} rows to {schema}.{table_name}")
    except Exception as e:
        print(f"  [DB Error] Pushing to {table_name}: {e}")