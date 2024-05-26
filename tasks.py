import logging

import pandas as pd
from celery import Celery
from celery.schedules import crontab

from config import CELERY_BROKER_URL, CELERY_RESULT_BACKEND
from main import Minio


app = Celery(
    "tasks",
    backend=CELERY_RESULT_BACKEND,
    broker=CELERY_BROKER_URL,
)


@app.task
def read_data_and_process(output_file):
    # Read Parquet data into a DataFrame
    try:
        pd.read_parquet(output_file)
        # TODO: Call pre process for training
        logging.info("Data sent for pre-process")
    except Exception as e:
        raise Exception(f"Error in: {e}")


@app.task(bind=True, acks_late=False, max_retries=5)
def start_app(self) -> None:
    try:
        Minio().execute()
    except Exception as exc:
        logging.error("applicant get error %s", exc, exc_info=True)
        raise self.retry(exc=exc)


app.conf.beat_schedule = {
    "run-every-minute": {
        "task": "tasks.start_app",
        "schedule": crontab(),
    },
}
