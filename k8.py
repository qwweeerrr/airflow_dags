from __future__ import annotations

import logging
import os

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff

log = logging.getLogger(__name__)

worker_container_repository = conf.get("kubernetes_executor", "worker_container_repository")
worker_container_tag = conf.get("kubernetes_executor", "worker_container_tag")

try:
    from kubernetes.client import models as k8s
except ImportError:
    log.warning(
        "The example_kubernetes_executor example DAG requires the kubernetes provider."
        " Please install it with: pip install apache-airflow[cncf.kubernetes]"
    )
    k8s = None

if k8s:
    with DAG(
        dag_id="example_kubernetes_executor",
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example3"],
    ) as dag:
        @task
        def run_snowflake_query():
            # Import required libraries for Snowflake connection and query execution
            from snowflake.sqlalchemy import URL, create_engine
            from sqlalchemy import text
            
            # Snowflake connection details
            snowflake_account = '<snowflake_account>'
            snowflake_user = '<snowflake_user>'
            snowflake_password = '<snowflake_password>'
            snowflake_warehouse = '<snowflake_warehouse>'
            snowflake_database = '<snowflake_database>'
            snowflake_schema = '<snowflake_schema>'
            
            # Snowflake query
            query = """
                SELECT *
                FROM <table_name>
                LIMIT 100
            """
            
            # Create Snowflake engine and execute the query
            engine = create_engine(
                URL(
                    account=snowflake_account,
                    user=snowflake_user,
                    password=snowflake_password,
                    warehouse=snowflake_warehouse,
                    database=snowflake_database,
                    schema=snowflake_schema,
                )
            )
            result = engine.execute(text(query))
            
            # Process the query result
            for row in result:
                print(row)
        
        snowflake_task = run_snowflake_query()

        start_task_executor_config = {
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}))
        }

        @task(executor_config=start_task_executor_config)
        def start_task():
            print_stuff()

        start_task() >> snowflake_task
