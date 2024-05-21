# ETL Pipeline using Apache Airflow and Kafka

This repository contains the code for a modern ETL pipeline built using Apache Airflow and Kafka as part of the IBM Professional Data Engineer Certification capstone project.

## Components

- **Apache Kafka**: For real-time data streaming and ingestion.
- **Apache Airflow**: For managing and orchestrating ETL workflows.


### Apache Airflow Pipeline

The ETL pipeline consists of the following tasks:

1. **unzip_data**: Unzips .tgz files containing raw data.
2. **extract_data_from_csv**: Extracts data from CSV files.
3. **extract_data_from_tsv**: Extracts data from TSV files.
4. **extract_data_from_fixed_width**: Extracts data from fixed-width text files.
5. **consolidate_data**: Merges data from multiple sources into a single DataFrame.
6. **transform_data**: Transforms the consolidated data and prepares it for loading into the data warehouse.


### Apache Kafka

...