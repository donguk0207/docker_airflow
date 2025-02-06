#import libs.
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum  # for local time zone +9time
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score
import pandas as pd

#Set timezone to Asia/Seoul
local_tz = pendulum.timezone("Asia/Seoul")

#default args
default_args = {
    "owner": "choe",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 4, tzinfo=local_tz),  # utc->local
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ML_Airflow_Practice",
    default_args=default_args,
    description="my airflow",
    schedule_interval=timedelta(days=1),
)

def feature_engineering(**kwargs):
    iris = load_iris()
    X = pd.DataFrame(iris["data"], columns=iris["feature_names"])
    y = pd.Series(iris["target"])

    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=47)

    #XCom을 활용해서 데이터 저장
    ti = kwargs["ti"]  #ti : task instance
    ti.xcom_push(key="x_train", value=x_train.to_json(orient="columns"))
    ti.xcom_push(key="x_test", value=x_test.to_json(orient="columns"))
    ti.xcom_push(key="y_train", value=y_train.to_json(orient="records"))
    ti.xcom_push(key="y_test", value=y_test.to_json(orient="records"))


#op_kwargs={"model_name": "RandomForest"}
#op_kwargs={"model_name": "GradientBoosting"}
def train_model(model_name, **kwargs):
    ti = kwargs["ti"]  # ti : task instance, for xcom
    x_train = pd.read_json(ti.xcom_pull(key="x_train", task_ids="feature_engineering"))
    x_test = pd.read_json(ti.xcom_pull(key="x_test", task_ids="feature_engineering"))
    y_train = pd.read_json(ti.xcom_pull(key="y_train", task_ids="feature_engineering"), typ="Series")
    y_test = pd.read_json(ti.xcom_pull(key="y_test", task_ids="feature_engineering"), typ="Series")

    if model_name == "RandomForest":
        model = RandomForestClassifier()
    elif model_name == "GradientBoosting":
        model = GradientBoostingClassifier()
    else:
        raise ValueError("Unsupported model:" + model_name)
    

    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)
    acc = accuracy_score(y_test, y_pred)

    ti.xcom_push(key=f"acc_{model_name}", value=acc)

def select_best_model(**kwargs):
    ti = kwargs["ti"]  # ti : task instance, for xcom
    rf_acc = ti.xcom_pull(key="acc_RandomForest", task_ids="train_rf")
    gb_acc = ti.xcom_pull(key="acc_GradientBoosting", task_ids="train_gb")

    best_model = "RandomForest" if rf_acc > gb_acc else "GradientBoosting"
    print(f"best model is {best_model} with accuracy {max(rf_acc, gb_acc)}")
    
    return best_model
    

with dag:
    t1 = PythonOperator(
        task_id="feature_engineering",
        python_callable=feature_engineering,
    )

    t2 = PythonOperator(
        task_id="train_rf",
        python_callable=train_model,
        op_kwargs={"model_name": "RandomForest"},
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="train_gb",
        python_callable=train_model,
        op_kwargs={"model_name": "GradientBoosting"},
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="select_best_model",
        python_callable=select_best_model,
        provide_context=True,
    )

    t1 >> [t2, t3] >> t4