from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag(schedule=None)
def dag_analysis():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def aggregating_data():
        import pandas as pd
        from datetime import datetime
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()

        with postgres_hook.connect() as conn:
            df = pd.read_sql(
                sql = "SELECT count(*) Total_Users FROM dibimbing.users",
                con = conn,
            )

        df.to_parquet("data/users_analysis.parquet", index=False)
        print("data analisis berhasil dibuat")


    @task
    def saving_analysis_data():
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()
        df            = pd.read_parquet("data/users_analysis.parquet")

        with postgres_hook.connect() as conn:
            df = df.to_sql(
                name      = "users_analysis",
                con       = conn,
                index     = False,
                schema    = "dibimbing",
                if_exists = "replace",
            )

        print("data berhasil disimpan")

    start_task >> aggregating_data() >> saving_analysis_data() >> end_task

dag_analysis()