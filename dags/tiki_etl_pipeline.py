import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from plugins import db

default_args = {
    "owner": "quang",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt_tiki"

# --------------- Categories --------------- #

def run_etl_category():
    from extract_and_load.raw_catogories import ROOT_ID, fetch_categories
    raw_json_list = fetch_categories(ROOT_ID)
    
    if not raw_json_list:
        print("No data fetched.")
        return
    
    db.push_df_to_db(
        df=pd.DataFrame(raw_json_list),
        table_name="raw_categories",
        schema="raw",
        primary_key="category_id",
    )

with DAG(
    "tiki_categories_etl",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=False,
) as categories_dag:
    
    crawl_and_load_categories_task = PythonOperator(
        task_id="crawl_tiki_categories", python_callable=run_etl_category
    )

    dbt_run_categories_task = BashOperator(
        task_id="dbt_transform_categories",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --select categories",
    )

    crawl_and_load_categories_task >> dbt_run_categories_task


# --------------- Products --------------- #

def run_etl_products():
    from extract_and_load.raw_products import fetch_products
    fetch_products(batch_size=100) 

with DAG(
    "tiki_products_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as products_dag:
    
    crawl_and_load_products_task = PythonOperator(
        task_id="crawl_tiki_products", 
        python_callable=run_etl_products
    )

    dbt_run_products_task = BashOperator(
        task_id="dbt_transform_produtcts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --select products",
    )

    crawl_and_load_products_task >> dbt_run_products_task


# --------------- Sellers --------------- #

def run_etl_sellers():
    from extract_and_load.raw_sellers import fetch_seller
    fetch_seller() 

with DAG(
    "tiki_seller_etl",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=False,
) as sellers_dag:
    
    crawl_and_load_seller_task = PythonOperator(
        task_id="crawl_tiki_seller", 
        python_callable=run_etl_sellers
    )

    dbt_run_seller_task = BashOperator(
        task_id="dbt_transform_sellers",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --select sellers",
    )

    crawl_and_load_seller_task >> dbt_run_seller_task

# --------------- reviews --------------- #

def run_etl_reviews():
    from extract_and_load.raw_reviews import fetch_reviews
    fetch_reviews(batch_size=100)

with DAG(
    "tiki_review_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as reviews_dag:
    
    crawl_and_load_review_task = PythonOperator(
        task_id="crawl_tiki_review", 
        python_callable=run_etl_reviews
    )

    dbt_run_review_task = BashOperator(
        task_id="dbt_transform_reviews",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --select reviews",
    )

    crawl_and_load_review_task >> dbt_run_review_task