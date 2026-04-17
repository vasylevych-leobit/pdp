import sys
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


sys.path.insert(0, os.path.dirname(__file__))

import task_1_generate_data as t1
import task_2_check_data as t2
import task_4_dq_checks as t4
import task_5_notifications as t5


DATABRICKS_CONN_ID = "databricks_default"
DATABRICKS_JOB_ID = 12345
ALERT_EMAILS = ["your-email@example.com"]
NUM_GENERATED_ROWS = 5_000


default_args = {
    "owner": "vlad-vasylevych",
    "email": ALERT_EMAILS,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": t5.on_failure_callback,
}


with DAG(
    dag_id="ecommerce_databricks_pipeline",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=t5.sla_miss_callback,
) as dag:

    generate_and_upload = PythonOperator(
        task_id="generate_and_upload_data",
        python_callable=t1.run,
        op_kwargs={
            "execution_date_str": "{{ execution_date.isoformat() }}",
            "num_rows": NUM_GENERATED_ROWS,
        },
    )

    check_data_available = PythonSensor(
        task_id="check_data_available",
        python_callable=t2.run,
        op_kwargs={
            "execution_date_str": "{{ execution_date.isoformat() }}",
        },
        poke_interval=60,
        timeout=1800,
        mode="reschedule",
    )


    trigger_databricks_pipeline = DatabricksRunNowOperator(
        task_id="trigger_databricks_pipeline",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "execution_date": "{{ execution_date.isoformat() }}",
            "year": "{{ execution_date.year }}",
            "month": "{{ '%02d' % execution_date.month }}",
            "day": "{{ '%02d' % execution_date.day }}",
            "hour": "{{ '%02d' % execution_date.hour }}",
        },
        sla=timedelta(minutes=50),
        wait_for_termination=True,
    )

    data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=t4.run,
        op_kwargs={
            "execution_date_str": "{{ execution_date.isoformat() }}",
        },
        sla=timedelta(minutes=10),
    )


    send_success_email = EmailOperator(
        task_id="send_success_email",
        to=ALERT_EMAILS,
        subject=(
            "Pipeline SUCCESS — ecommerce_databricks_pipeline "
            "{{ execution_date.strftime('%Y-%m-%d %H:00 UTC') }}"
        ),
        html_content="""
        <html><body>
          <h2 style='color:green'>Pipeline completed successfully</h2>
          <p><b>DAG:</b> ecommerce_databricks_pipeline</p>
          <p><b>Execution date:</b> {{ execution_date.strftime('%Y-%m-%d %H:%M UTC') }}</p>
          <p>All tasks — data generation, availability check, Spark pipeline,
             and data quality — passed without errors.</p>
        </body></html>
        """,
    )

    (
        generate_and_upload
        >> check_data_available
        >> trigger_databricks_pipeline
        >> data_quality_checks
        >> send_success_email
    )
