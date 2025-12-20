from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pendulum
from datetime import timedelta
from api.video_stats import get_playlist_Id, extract_video_data, get_videos_ids, save_to_json


from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

local_tz = pendulum.timezone("UTC")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email": ["data@engineers.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": pendulum.datetime(2025, 1, 1, tz=local_tz),
    "dagrun_timeout": timedelta(hours=1),
}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON with raw data",
    schedule="0 14 * * *",
    catchup=False,
) as dag_produce:

    # dependencies
    playlist_id = get_playlist_Id()
    video_ids = get_videos_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id = "trigger_update_db",
        trigger_dag_id = "update_db",
    )

    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into bioth staging and core schemas",
    schedule=None,
    catchup=False,
) as dag_update:

    # define tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id = "trigger_data_quality",
        trigger_dag_id = "data_quality", 
    )

    update_staging >> update_core >> trigger_data_quality

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check data quality on both layers in the db",
    schedule=None,
    catchup=False,
) as dag_quality:

    soda_validate_staging = yt_elt_data_quality(
        schema=staging_schema,
        checks_file="checks_staging.yml"
    )

    soda_validate_core = yt_elt_data_quality(
        schema=core_schema,
        checks_file="checks_core.yml"
    )

    soda_validate_staging >> soda_validate_core