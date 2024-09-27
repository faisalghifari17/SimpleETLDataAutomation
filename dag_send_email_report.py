from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

@dag(schedule_interval="0 9 * * *", start_date=datetime(2024, 9, 1), catchup=False)
def dag_send_email_report():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    @task
    def loading_analysis_data():
        postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()

        # Fetch the analysis data from PostgreSQL
        with postgres_hook.connect() as conn:
            df = pd.read_sql(
                sql="SELECT * FROM dibimbing.users_analysis",  # Ensure the table exists and has data
                con=conn,
            )

        # Convert the DataFrame to an HTML table
        html_table = df.to_html(index=False, border=1, classes='dataframe', justify='center')
        print("DataFrame converted to HTML table.")
        return html_table

    @task
    def sending_email(html_table):
        # Fetch email recipient from Airflow Variables, set a default if not set
        email_to = Variable.get("report_email_to", default_var='faisalzghifari@gmail.com')

        # Create the email content with the HTML table embedded
        email_content = f"""
        <h3>Daily Analysis Report</h3>
        <p>Here is the latest analysis report from the database:</p>
        {html_table}
        <p>Regards,<br>Airflow Reporting System</p>
        """

        # Configure the EmailOperator
        send_email = EmailOperator(
            task_id='send_email',
            to=email_to,
            subject='Daily Analysis Report',
            html_content=email_content  # Embed the HTML content directly in the email
        )

        # Execute the EmailOperator to send the email
        send_email.execute(context=None)  # Run it directly within the Airflow task

    # Chain the tasks to load and send the email
    html_table = loading_analysis_data()
    start_task >> html_table >> sending_email(html_table) >> end_task

dag_send_email_report()
