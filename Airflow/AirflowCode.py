from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

new_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'num_workers': 4,
    'node_type_id': 'i3.xlarge',
}

notebook_task = {
    'notebook_path': '/Users/raghav.neeraj81@gmail.com/librarydataTrans2Pub',
}



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('databricks_dag_DB_Trans2Pub',
    start_date=datetime(2022, 1, 18),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='DataBricksID',
        new_cluster=new_cluster,
        notebook_task=notebook_task
    )
    
    opr_submit_run