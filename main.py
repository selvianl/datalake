import io
import logging
import os
import time
from datetime import datetime

import boto3
import botocore
import botocore.exceptions
import pandas as pd
import redis
from sqlalchemy import create_engine, text

from config import (
    BUCKETNAME,
    ENDPOINT,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    REDIS_DB,
    REDIS_PORT,
    REDIS_URL,
    TABLE_NAME,
    XLSX_FILE,
)


class Minio:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.table_name = TABLE_NAME
        self.parquet_filename = (
            self.table_name
            + " "
            + datetime.today().strftime("%Y-%m-%d : %H-%M")
        )
        self.output_file = self.table_name + ".output_file"
        self.xlsx_file = XLSX_FILE

        # redis
        self.r = redis.Redis(host=str(REDIS_URL), port=int(REDIS_PORT), db=int(REDIS_DB))  # type: ignore # type: ignore

        self.engine = self.connect_postgres()
        self.client = self.minio_client()

    def connect_postgres(self):
        """Connects to the PostgreSQL database and returns a SQLAlchemy engine."""
        connection_string = (
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )

        return create_engine(connection_string)

    def write_db(self):
        try:
            df = pd.read_excel(self.xlsx_file)
            df.columns = [
                "transaction_start",
                "agent",
                "transaction_end",
                "text",
                "browser",
            ]
            df["text"] = df["text"].str[:500]  # Trim text fields

            # Insert data into the table atomically
            with self.engine.begin() as connection:
                df.to_sql(
                    self.table_name,
                    self.engine,
                    index=False,
                    if_exists="replace",
                )
                connection.execute(text("COMMIT"))
            self.logger.info("Data inserted to db successfully!")
            self.r.set("table_created", "true")
            self.r.set(self.table_name, "true")

        except Exception as e:
            self.logger.error(f"Error inserting data: {e}")

    def minio_client(self):
        return boto3.client(
            "s3",
            endpoint_url=f"http://{ENDPOINT}:9000",
            aws_access_key_id=MINIO_ROOT_USER,
            aws_secret_access_key=MINIO_ROOT_PASSWORD,
        )

    def extract_data(self):
        self.r.set("started", "true")
        try:
            df = pd.DataFrame()
            offset, limit = 0, 10000

            while self.r.get("df_empty") is None:
                query = f"select * from {self.table_name} offset {offset} limit {limit}"

                with self.engine.connect() as connection:
                    temp_df = pd.read_sql(query, connection)

                    if temp_df.empty:
                        self.logger.info("No more data to extract")
                        self.r.set("df_empty", "true")
                        break

                    df = pd.concat([df, temp_df], ignore_index=True)

                offset += limit

            self.r.delete("df_empty")
            self.r.delete("started")
            return df

        except Exception as e:
            raise e

    def upload_to_minio(self, data):
        """Uploads the data in Parquet format to Minio using boto3."""
        try:
            buffer = io.BytesIO()
            data.to_parquet(buffer, index=False)
            self.client.put_object(
                Bucket=BUCKETNAME,
                Key=self.parquet_filename,
                Body=buffer.getvalue(),
            )
            self.logger.info("Parquet file uploaded to s3")
        except Exception as e:
            raise e

    def execute(self):
        try:
            if self.r.get("started"):
                self.logger.info("There is started process already")
                return

            if self.r.get(self.table_name):
                self.logger.info("This file is already inserted")
                return

            if not self.r.get("table_created"):
                if not os.path.exists(self.xlsx_file):
                    raise Exception(f"File {self.xlsx_file} does not exist.")
                self.write_db()

            df = self.extract_data()

            self.upload_to_minio(df)

            is_exists = False
            while not is_exists:
                try:
                    self.client.head_object(
                        Bucket=BUCKETNAME, Key=self.parquet_filename
                    )
                    is_exists = True
                    self.logger.info("Object is ready for download")
                except botocore.exceptions.ClientError as e:
                    is_exists = False
                    time.sleep(5)
                    self.logger.info("Object is not ready for download")

            self.client.download_file(
                BUCKETNAME, self.parquet_filename, self.output_file
            )
            from tasks import read_data_and_process

            read_data_and_process.delay(self.output_file)
            self.r.delete("started")
        except Exception as e:
            self.r.delete("started")
            raise e
