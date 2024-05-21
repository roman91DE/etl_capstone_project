from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
import tarfile



project_path = Path.home() /"repos" / "etl_capstone_project" /"airflow" /"dags" / "finalassignment" 

if not project_path.exists():
    project_path = Path("/home/project") /"airflow" /"dags" / "finalassignment" 
    assert project_path.exists(), F"no valid project path exists"

def unzip_data() -> None:
    """
    Unzips .tgz files in the 'staging' directory of the project path.
    Extracts contents to a directory named after each tar file (without extension).
    """
    staging_path = project_path / "staging"
    tar_files = staging_path.glob("*.tgz")
    
    for tgz_file in tar_files:
        extract_path = staging_path / tgz_file.stem
        with tarfile.open(tgz_file, "r:gz") as tar:
            tar.extractall(path=extract_path)
            print(f"Extracted {tgz_file} to {extract_path}")


def dummy_task_2():
    """Dummy task 2 function."""
    print("Executing dummy task 2") 

def dummy_task_3():
    """Dummy task 3 function."""
    print("Executing dummy task 3")

default_args = {
    'owner': 'Captain Cool',
    'email': ["captain@cool.de"],
    "start_date": datetime.today(),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
) as dag:
    
    task_1 = PythonOperator(
        task_id='unzip_task',
        python_callable=unzip_data,
    )


    task_1
