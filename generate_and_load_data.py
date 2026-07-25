import logging
import os
import random
from typing import Tuple
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "deloitte-de-home-ty"
DATASET_ID = "raw_data"
NUM_RECORDS = 1000
TARGET_SKEW_CITY = "Tel Aviv"
CITIES_POOL = ["Ramat Gan", "Haifa", "Jerusalem", "Beer Sheva"]


def generate_mock_data(num_records: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logging.info("Starting synthetic data generation...")

    address_records = []
    user_records = []

    for i in range(1, num_records + 1):
        address_id = f"ADDR_{i:04d}"
        user_id = f"USR_{i:04d}"

        if i <= (num_records // 2):
            city = TARGET_SKEW_CITY
        else:
            city = random.choice(CITIES_POOL)

        street = f"Street {random.randint(1, 100)}"

        address_records.append([address_id, street, city])
        user_records.append([user_id, f"User_{i}", address_id])

    df_addresses = pd.DataFrame(
        address_records, columns=["address_id", "street", "city"]
    )
    df_users = pd.DataFrame(
        user_records, columns=["user_id", "name", "address_id"]
    )

    logging.info(
        f"Data generated successfully. Skew check for '{TARGET_SKEW_CITY}': "
        f"{len(df_addresses[df_addresses['city'] == TARGET_SKEW_CITY])} records out of {num_records}."
    )
    return df_users, df_addresses


def save_data_locally(
    df_users: pd.DataFrame,
    df_addresses: pd.DataFrame,
    output_dir: str = "data",
) -> Tuple[str, str]:
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    users_path = os.path.join(output_dir, "users.csv")
    addresses_path = os.path.join(output_dir, "addresses.csv")

    df_users.to_csv(users_path, index=False)
    df_addresses.to_csv(addresses_path, index=False)

    logging.info(f"Local backups saved to: {users_path} and {addresses_path}")
    return users_path, addresses_path


def load_dataframe_to_bigquery(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_name: str,
    dataset_id: str,
    project_id: str,
) -> None:
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    logging.info(f"Initiating BigQuery load job for destination: {table_ref}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()
        logging.info(
            f"Successfully loaded {job.output_rows} rows into {table_ref}"
        )
    except GoogleCloudError as gcp_err:
        logging.error(f"Failed to load data to BigQuery table {table_name}: {gcp_err}")
        raise
    except Exception as err:
        logging.error(f"An unexpected error occurred during ingestion: {err}")
        raise


def main():
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)

        df_users, df_addresses = generate_mock_data(num_records=NUM_RECORDS)

        save_data_locally(df_users, df_addresses)

        load_dataframe_to_bigquery(
            bq_client, df_users, "users", DATASET_ID, PROJECT_ID
        )
        load_dataframe_to_bigquery(
            bq_client, df_addresses, "addresses", DATASET_ID, PROJECT_ID
        )

        logging.info("Pipeline execution completed successfully!")

    except Exception as pipeline_error:
        logging.critical(f"Pipeline execution halted: {pipeline_error}")


if __name__ == "__main__":
    main()