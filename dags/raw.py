from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import logging

# 기본 Dag 설정
default_args = {
    'owner': 'hongkyu'
}

#bigquery_conn = Connection.get_connection_from_secrets(conn_id='bigquery')
hook = BigQueryHook(gcp_conn_id='bigquery')

# myhomepagedata.analytics_292326214.events_20240417
"""해당 테이블이 존재하는지 확인"""
def check_table_exists(dataset_id, table_id):
    client = hook.get_client()
    try:
        client.get_table(f"{dataset_id}.{table_id}")
        logging.info(f"Table {table_id}가 존재합니다.")
        return True
    except Exception as e:
        logging.info(f"Table {table_id}가 존재하지 않습니다.")
        return False

"""해당 테이블이 존재하면, 해당 테이블을 합치기"""


"""합친 후에 쿼리 성능 보고"""


"""dimensional modeling 작성 후"""


"""테이블 파티셔닝"""






with DAG(
    default_args=default_args,
    dag_id = "is_exist_table",
    start_date=pendulum.datetime(2024, 4, 1, tz="Asia/Seoul"),
    schedule = "0 9 * * *",
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id="task1",
        python_callable=check_table_exists,
        op_kwargs={
            "dataset_id": 'myhomepagedata.analytics_292326214',
            'table_id': 'events_20240417'
        }
    )








# with DAG(
#     dag_id = "is_exist_table",
#     start_date=pendulum.datetime(2024, 4, 1, tz="Asia/Seoul"),
#     schedule = "0 9 * * *",
#     catchup=False
# ) as dag:
#     client = bigquery.Client()
#
#     """해당 테이블이 bigquery에 존재하는지 확인"""
#     def check_table_exists(dataset_id, table_id):
#         try:





