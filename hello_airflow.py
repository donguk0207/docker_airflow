#import libs.
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum #for local time zone +9time

#Set timezone to Asia/Seoul
local_tz = pendulum.timezone("Asia/Seoul")

#default args
default_args = {
    "owner":"choe",
    "depends_on_past":False,
    "start_date":datetime(2025, 2, 3, tzinfo=local_tz), #utc->local
    "email_on_retry":False,
    "email_on_failure":False,
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}

# DAG 정의
dag = DAG(
    "first_airflow_practice",  # DAG ID (공백 금지)
    default_args=default_args,
    description="My airflow DAG",
    schedule_interval=timedelta(days=1),
    tags=["example", "practice", "test"]
)

input_words = "python very easy, airflow more.... ^^"

def review_word(word):
    print(word)


#Task impl.
previous_task = None
for i, word in enumerate(input_words.split()):
    task = PythonOperator(
        task_id=f"word_{i}",
        python_callable=review_word,
        op_kwargs={"word":word},
        dag=dag
    )

    if previous_task:
        previous_task >> task
    previous_task = task
