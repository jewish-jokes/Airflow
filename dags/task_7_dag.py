import datetime
import re

import pandas as pd

from airflow.decorators import dag, task, task_group
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sensors.filesystem import FileSensor

AIRFLOW_HOME = "/opt/airflow"
DATA_INPUT = f"{AIRFLOW_HOME}/data/raw"
DATA_OUTPUT = f"{AIRFLOW_HOME}/data/proc"
FILENAME = "tiktok_google_play_reviews.csv"


@dag(
    dag_id="task_7_dag",
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=["toMongo", "pandas"],
)
def task_7_dag():
    def download(flag: bool) -> str:
        """
        Simulates downloading process of main file and return path/to/file
        """
        if flag:
            return f"{DATA_INPUT}/{FILENAME}"

    @task(task_id="fill_nulls")
    def fill_nulls(file) -> str:
        df = pd.read_csv(file)
        df["content"] = df["content"].fillna("-")
        output = f"{DATA_OUTPUT}/tiktok_without_nulls.csv"
        df.to_csv(output, index=False)
        return output

    @task(task_id="replace_emojis_content")
    def replace_emojis_content(file) -> str:
        df = pd.read_csv(file)
        df["content"] = df["content"].str.replace(
            "[^A-Za-z0-9_ ]", "", flags=re.UNICODE
        )
        output = f"{DATA_OUTPUT}/tiktok_without_nulls_and_emojis.csv"
        df.to_csv(output, index=False)
        return output

    @task(task_id="sort_by_created_at")
    def sort_by_created_at(file) -> str:
        df = pd.read_csv(file)
        df.sort_values("at", inplace=True)
        output = f"{DATA_OUTPUT}/ordered_tiktok_without_nulls_and_emojis.csv"
        df.to_csv(output, index=False)
        return output

    @task_group(group_id="clean_data_task_group")
    def clean_data_task_group(file: str) -> str:
        return sort_by_created_at(fill_nulls(replace_emojis_content(file)))

    @task(task_id="upload_to_mongo")
    def upload_to_mongo(file) -> None:
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.task_7_db
        tik_tok_comments = db.tik_tok_comments
        df = pd.read_csv(file)
        df.reset_index(inplace=True)
        data_dict = df.to_dict("records")
        tik_tok_comments.insert_many(data_dict)

    file_sensor = FileSensor(
        task_id="file_sensor",
        filepath=f"{DATA_INPUT}/{FILENAME}",
        fs_conn_id="my_fs",
        poke_interval=20,
    )

    upload_to_mongo(clean_data_task_group(download(file_sensor)))


task_7_dag()
