from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import time
import pandas
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
import sqlalchemy
import logging

def insert_data(table_name):
    df = pandas.read_csv(f"/11/{table_name}.csv", delimiter=";", encoding='utf-8')
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    print(df.columns)
    
    if table_name == "ds.ft_balance_f":
        inspector = sqlalchemy.inspect(engine)
        primary_key_columns = [column.name for column in inspector.get_pk_constraint(table_name)['constrained_columns']]
        
        if primary_key_columns:
           for row in df.to_dict(orient="records"):
                insert_stmt = insert(sqlalchemy.Table(table_name, sqlalchemy.MetaData(), autoload_with=engine)).values(row)
                try:
                   do_update_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=primary_key_columns,
                        set_=row
                   )
                   engine.execute(do_update_stmt)
                except Exception as e:
                   logging.error(f"Error inserting/updating row: {row}. Error: {e}")
        else:
             df.to_sql(table_name, engine, schema="ds", if_exists="replace", index=False)
    else:
        df.to_sql(table_name, engine, schema="ds", if_exists="replace", index=False)


def insert_start_time(ti):
    """Записывает время начала DAG в xcom"""
    current_time = datetime.now()
    ti.xcom_push(key='start_time', value=current_time)

def insert_end_time(ti):
    """Записывает время окончания DAG в xcom, доставая время старта"""
    current_time = datetime.now()
    start_time = ti.xcom_pull(key='start_time', task_ids='insert_start_time_log')
    ti.xcom_push(key='end_time', value=current_time)
    ti.xcom_push(key='all_times', value={"start_time": start_time, "end_time": current_time})
    
def insert_status(ds, **context):
    """Записывает время старта, время конца и статус в таблицу logs.airflow_dag_logs"""
    postgres_hook = PostgresHook("postgres-db")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    dag_run = context.get('dag_run')
    status = dag_run.get_state()
    
    ti = context['ti']
    times = ti.xcom_pull(key='all_times', task_ids='insert_end_time_log')
    start_time = times['start_time']
    end_time = times['end_time']
    

    sql_query = f"""
        INSERT INTO logs.airflow_dag_logs (start_time, end_time, status)
        VALUES ('{start_time}', '{end_time}', '{status}');
    """
    cursor.execute(sql_query)
    conn.commit()
    cursor.close()
    conn.close()

def pause_task():
    """Пауза на 5 секунд"""
    time.sleep(5)

default_args = {
    "owner": "vtuzhikov",
    "start_date": datetime(2024, 2, 25),
    "retries": 2,
}

with DAG(
    "insert_data",
    default_args=default_args,
    description="Загрузка данных в ds",
    catchup=False,
    schedule="0 0 * * *",
) as dag:
    insert_start_time_task = PythonOperator(
        task_id="insert_start_time_log",
        python_callable=insert_start_time,
    )
    
    pause = PythonOperator(
        task_id="pause_5_seconds",
        python_callable=pause_task
    )

    start = DummyOperator(task_id="start")

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_balance_f"},
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_posting_f"},
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_account_d"},
    )
    
    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_currency_d"},
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_exchange_rate_d"},
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_ledger_account_s"},
    )
    
    insert_end_time_task = PythonOperator(
        task_id="insert_end_time_log",
        python_callable=insert_end_time,
    )
    
    insert_status_task = PythonOperator(
        task_id="insert_status_log",
        python_callable=insert_status,
    )


    insert_start_time_task >> pause >> start >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s] >> insert_end_time_task >> insert_status_task
