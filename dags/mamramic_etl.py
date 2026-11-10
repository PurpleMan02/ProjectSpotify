from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

JAR = "java -jar /opt/airflow/jars/mamramic-etl.jar"

with DAG(
    dag_id="mamramic_etl",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
) as dag:

    bronze = BashOperator(task_id="bronze", bash_command=f"{JAR} bronze")
    silver = BashOperator(task_id="silver", bash_command=f"{JAR} silver")
    gold   = BashOperator(task_id="gold",   bash_command=f"{JAR} gold")

    bronze >> silver >> gold