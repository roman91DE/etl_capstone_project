from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
import tarfile
import pandas as pd


project_path = (
    Path.home()
    / "repos"
    / "etl_capstone_project"
    / "airflow"
    / "dags"
    / "finalassignment"
)

if not project_path.exists():
    project_path = Path("/home/project") / "airflow" / "dags" / "finalassignment"
    assert project_path.exists(), f"no valid project path exists"


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


def extract_data_from_csv() -> None:
    """
    The function performs the following steps:
    1. Locates the CSV file named 'vehicle-data.csv' in the 'tolldata' subdirectory within the 'staging' directory.
    2. Defines the headers to be used for reading the CSV file.
    3. Reads the CSV file into a pandas DataFrame, ensuring all columns are read as strings.
    4. Filters the DataFrame to keep only the specified columns.
    5. Saves the filtered DataFrame to a new CSV file named 'csv_data.csv' in the project path.

    Raises:
        FileNotFoundError: If the 'vehicle-data.csv' file does not exist in the specified directory.

    """
    data_path = project_path / "staging" / "tolldata"
    csv_file = list(data_path.glob("vehicle-data.csv"))[0]

    headers = [
        "Rowid",
        "Timestamp",
        "Anonymized Vehicle number",
        "Vehicle type",
        "Number of axles",
        "Vehicle code",
    ]

    keep_cols = ["Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type"]

    df = pd.read_csv(csv_file, dtype=str, sep=",", names=headers)
    df.filter(items=keep_cols).to_csv(project_path / "csv_data.csv", index=False)


def extract_data_from_tsv():
    """
    The function performs the following steps:
    1. Locates the CSV file named 'tollplaza-data.tsv' in the 'tolldata' subdirectory within the 'staging' directory.
    2. Defines the headers to be used for reading the CSV file.
    3. Reads the CSV file into a pandas DataFrame, ensuring all columns are read as strings.
    4. Filters the DataFrame to keep only the specified columns.
    5. Saves the filtered DataFrame to a new CSV file named 'tsv_data.csv' in the project path.

    Raises:
        FileNotFoundError: If the 'vehicle-data.csv' file does not exist in the specified directory.

    """
    data_path = project_path / "staging" / "tolldata"
    csv_file = list(data_path.glob("tollplaza-data.tsv"))[0]

    headers = [
        "Rowid",
        "Timestamp",
        "Anonymized Vehicle number",
        "Vehicle type",
        "Number of axles",
        "Tollplaza id",
        "Tollplaza code",
    ]

    keep_cols = ["Number of axles", "Tollplaza id", "Tollplaza code"]

    df = pd.read_csv(csv_file, dtype=str, sep="\t", names=headers)
    df.filter(items=keep_cols).to_csv(project_path / "tsv_data.csv", index=False)


def extract_data_from_fixed_width():
    """
    The function performs the following steps:
    1. Locates the TXT file named 'payment-data.txt' in the 'tolldata' subdirectory within the 'staging' directory.
    2. Defines the headers to be used for reading the TXT file.
    3. Reads the TXT file into a pandas DataFrame, ensuring all columns are read as strings.
    4. Filters the DataFrame to keep only the specified columns.
    5. Saves the filtered DataFrame to a new CSV file named 'fixed_width_data.csv' in the project path.

    Raises:
        FileNotFoundError: If the 'vehicle-data.csv' file does not exist in the specified directory.

    """
    data_path = project_path / "staging" / "tolldata"
    txt_file = list(data_path.glob("payment-data.txt"))[0]

    headers = [
        "Rowid",
        "Timestamp",
        "Anonymized Vehicle number",
        "Tollplaza id",
        "Tollplaza code",
        "Type of Payment code",
        "Vehicle Code",
    ]

    keep_cols = ["Type of Payment code", "Vehicle Code"]

    df = pd.read_fwf(txt_file, dtype=str, names=headers)
    df.filter(items=keep_cols).to_csv(
        project_path / "fixed_width_data.csv", index=False
    )



def consolidate_data():
    """
    Consolidates data from three different CSV files into a single CSV file.

    This function performs the following steps:
    1. Reads data from three CSV files located in the project path:
       - 'csv_data.csv'
       - 'tsv_data.csv'
       - 'fixed_width_data.csv'
    2. Reads each CSV file into a pandas DataFrame, ensuring all data is read as strings.
    3. Defines the headers to be used for the final consolidated DataFrame.
    4. Concatenates the three DataFrames column-wise and selects only the specified headers.
    5. Saves the consolidated DataFrame to a new CSV file named 'extracted_data.csv' in the project path.

    Raises:
        FileNotFoundError: If any of the input CSV files do not exist.
    """

    file1 = project_path / "csv_data.csv"
    file2 = project_path / "tsv_data.csv"
    file3 = project_path / "fixed_width_data.csv"

    df1 = pd.read_csv(file1, dtype=str, sep=",")
    df2 = pd.read_csv(file2, dtype=str, sep=",")
    df3 = pd.read_csv(file3, dtype=str, sep=",")

    headers = [
        "Rowid",
        "Timestamp",
        "Anonymized Vehicle number",
        "Vehicle type",
        "Number of axles",
        "Tollplaza id",
        "Tollplaza code",
        "Type of Payment code",
        "Vehicle Code"
    ]


    merged_df = pd.concat([df1, df2, df3], axis=1).loc[:, headers]

    merged_df.to_csv(project_path / "extracted_data.csv", index=False)







default_args = {
    "owner": "Captain Cool",
    "email": ["captain@cool.de"],
    "start_date": datetime.today(),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
) as dag:

    task_1 = PythonOperator(
        task_id="unzip_task",
        python_callable=unzip_data,
    )

    task_1
