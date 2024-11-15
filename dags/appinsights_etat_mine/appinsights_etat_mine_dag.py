"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://www.astronomer.io/docs/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime

import os
import json
import pandas as pd
from datetime import datetime
from jinja2 import Template
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
from includes.application_insight import AppInsightsClientBuilder, AppInsightsClient

def get_etat_last_timestamp() -> datetime:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()
    return datetime(2024, 10, 25, 8, 0, 0, 813)

def get_new_rows(last_datetime:datetime) -> pd.DataFrame:
    AAD_ID = os.getenv('AAD_ID', '')
    AAD_SECRET = os.getenv('AAD_SECRET', '')
    SCOPES = os.getenv('SCOPES', 'https://api.applicationinsights.io/.default').split(',')
    APPLICATION_INSIGHTS_ID = os.getenv('APPLICATION_INSIGHTS_ID', '')
    APPLICATION_INSIGHTS_API_KEY = os.getenv('APPLICATION_INSIGHTS_API_KEY', '')
    MICROSOFT_AUTHORITY_ID = os.getenv.get('MICROSOFT_AUTHORITY_ID', '')

    Builder = AppInsightsClientBuilder(AAD_id=AAD_ID, AAD_secret=AAD_SECRET, ApplicationInsights_ID=APPLICATION_INSIGHTS_ID, AzureAuthorityId=MICROSOFT_AUTHORITY_ID)
    AppClient:AppInsightsClient = Builder.build()

    # --- customEvents --- #
    # Load the KQL query template
    with open('queries/timestamp-between.kql', 'r') as file:
        query_template = Template(file.read())
    # Define parameters
    params = {
        "StartDate": last_datetime,
        "EndDate": datetime.now(),
        "TableName": "customEvents",
        "condition" : 'name == "send_editionCO_service" and timestamp > datetime("2024-10-25, 8:00:00.813")'
    }
    # Render the query with parameters
    query = query_template.render(**params)
    return AppClient.query_as_df(query)

def inject_rows_etatEvents(data:pd.DataFrame, tableName:str):
    pass

with dag(
    dag_id="appinsights_etat_mine_dag",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etatEvents", "customEvents", "etat"]
) as appinsights_etat_mine_dag:
    last_datetime = get_etat_last_timestamp()
    new_rows:pd.DataFrame = get_new_rows(last_datetime=last_datetime)
    inject_rows_etatEvents(data=new_rows, tableName="customEvents")
