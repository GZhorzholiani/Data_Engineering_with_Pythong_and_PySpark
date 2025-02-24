from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'MovieProcessing',
    default_args=default_args,
    description='Processing Movies.txt with spark',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    file_sensor = FileSensor(
        task_id='check_for_Movies_txt',
        filepath='/home/nika/Desktop/MoviesFolder/Movies.txt',
    )

    spark_batch_process = SSHOperator(
        task_id='spark_batch_process',
        ssh_conn_id='ssh_default',
        command='/opt/spark/bin/spark-submit /home/ubuntu/NM/Movie_processing_from_txt_to_parquet.py',
    )

    remove_file = BashOperator(
        task_id='remove_file',
        bash_command="rm /home/nika/Desktop/MoviesFolder/Movies.txt"
    )

    spark_producer = SSHOperator(
        task_id='spark_producer',
        ssh_conn_id='ssh_default',
        command='/opt/spark/bin/spark-submit /home/ubuntu/NM/Movie_processing_from_txt_to_parquet.py',
    )

    spark_consumer = SSHOperator(
        task_id='spark_consumer',
        ssh_conn_id='ssh_default',
        command='/opt/spark/bin/spark-submit /home/ubuntu/NM/Movie_processing_from_txt_to_parquet.py',
    )

    file_sensor >> spark_batch_process >> remove_file >> spark_producer >> spark_consumer
