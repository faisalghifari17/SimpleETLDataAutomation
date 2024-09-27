from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(schedule_interval="0 7 * * *", start_date=datetime(2024,9,1), catchup=False)
def dag_etl():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def extract_from_mysql():
        import pandas as pd
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        mysql_hook = MySqlHook("mysql_dibimbing").get_sqlalchemy_engine()

        with mysql_hook.connect() as conn:
            df = pd.read_sql(
                sql = "SELECT * FROM dibimbing.users",
                con = conn,
            )

        df.to_parquet("data/users.parquet", index=False)
        print("data berhasil diextract")


    @task
    def load_to_postgres():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()
        df            = pd.read_parquet("data/users.parquet")

        with postgres_hook.connect() as conn:
            df = df.to_sql(
                name      = "users",
                con       = conn,
                index     = False,
                schema    = "dibimbing",
                if_exists = "replace",
            )

        print("data berhasil diload")

    trigger_dag_analysis = TriggerDagRunOperator(
        task_id = "trigger_dag_analysis",
        trigger_dag_id = "dag_analysis",
        wait_for_completion = False   
        )

    start_task >> extract_from_mysql() >> load_to_postgres() >> trigger_dag_analysis >> end_task

dag_etl()