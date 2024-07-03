from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import os

# 서비스 계정 키 파일 경로 설정
credential_path = "../include/gcp/service_account.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# 프로젝트와 데이터셋 정보
project_id = "myhomepagedata"
dataset_id = 'analytics_292326214'
destination_table = f'{project_id}.my_sess'

# DAG 정의
dag = DAG(
    "rawdata_to_new_table",
    default_args={
        "owner": "hongkyu",
        "start_date": days_ago(1),
        "retries": 1,
    },
    schedule_interval="@daily",
    catchup=False,
)

# BigQueryHook 생성
def create_bigquery_hook():
    return BigQueryHook(gcp_conn_id="bigquery")

# 시작 및 종료 Operator 정의
start_task = EmptyOperator(
    task_id='start',
    dag=dag
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag
)

# 마지막으로 처리된 테이블 날짜를 가져오는 작업
def get_last_processed_date(**kwargs):
    last_processed_date = kwargs['ti'].xcom_pull(
        task_ids='copy_tables',
        key='last_processed_date',
        default=None
    )
    return last_processed_date

get_last_processed_date_task = PythonOperator(
    task_id='get_last_processed_date',
    python_callable=get_last_processed_date,
    provide_context=True,
    dag=dag
)

# 테이블 목록을 가져오는 작업
def get_table_list(after_date=None, **kwargs):
    bigquery_hook = create_bigquery_hook()

    if after_date:
        # 특정 날짜 이후의 테이블 목록 쿼리
        # 여기가 잘못됨. 왜 이딴 식으로 해야하지?
        # bigquery에서 테스트해봐야함.
        query = f"""
                SELECT table_name
                FROM `{project_id}.INFORMATION_SCHEMA.TABLES`
                WHERE table_catalog = '{project_id}' AND table_schema = '{dataset_id}'
                AND table_name LIKE 'events_%' AND table_name > 'events_{after_date}'
                ORDER BY table_name ASC
                LIMIT 10
                """

    else:
        # 초기 실행 시 모든 테이블 목록 쿼리
        query = f"""
                SELECT table_name
                FROM `{project_id}.{dataset_id}.*`
                WHERE table_catalog = '{project_id}' AND table_schema = '{dataset_id}'
                AND table_name LIKE 'events_%'
                ORDER BY table_name ASC
                LIMIT 10
                """

    # 테이블 목록 쿼리를 실행하여 테이블 이름 리스트를 반환
    tables = bigquery_hook.get_pandas_df(query)
    table_names = tables['table_id'].tolist()
    return table_names

get_table_list_task = PythonOperator(
    task_id='get_table_list',
    python_callable=get_table_list,
    provide_context=True,
    dag=dag
)

# BigQueryExecuteQueryOperator를 사용하여 데이터 삽입 쿼리 실행
def execute_query(**kwargs):
    ti = kwargs['ti']
    last_processed_date = ti.xcom_pull(
        task_ids="get_last_processed_date",
        key='last_processed_date', default=None
    )
    table_names = get_table_list(after_date=last_processed_date, ti=ti)
    bigquery_hook = create_bigquery_hook()

    for table_name in table_names:
        query = f"""
                INSERT INTO `{destination_table}`
                SELECT
                    value.int_value as sess_id,  -- 세션 아이디
                    user_pseudo_id as user_id,  -- 유저 아이디
                    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS visit_stime, -- 방문 시간
                    event_date as visit_date,  -- 방문 날짜
                    traffic_source.source as traffic_source,  -- 트래픽 발생위치
                    traffic_source.medium as traffic_medium,  -- 트래픽 매개체
                    device.category as device_category,
                    device.operating_system as device_os,
                    geo.city as geo_city
                FROM `{project_id}.{dataset_id}.{table_name}`,
                UNNEST(event_params)
                WHERE key = 'ga_session_id'
                """

        # BigQueryExecuteQueryOperator를 사용하여 데이터 삽입 쿼리 실행
        execute_task = BigQueryExecuteQueryOperator(
            task_id=f'execute_query_{table_name}',
            sql=query,
            use_legacy_sql=False,
            bigquery_conn_id="bigquery",
            dag=dag
        )
        execute_task.execute(context=kwargs)

    if table_names:
        last_processed_table = table_names[-1]
        last_processed_date = last_processed_table.split('_')[1]
        ti.xcom_push(key='last_processed_date', value=last_processed_date)

execute_query_task = PythonOperator(
    task_id='execute_query',
    python_callable=execute_query,
    provide_context=True,
    dag=dag
)

# Task 간의 의존성 설정
start_task >> get_last_processed_date_task >> get_table_list_task >> execute_query_task >> end_task
