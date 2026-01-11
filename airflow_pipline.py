from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import time
import json
import logging
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    filename='wordstat.log',
    format='%(asctime)s,'
           ' %(levelname)s,'
           ' %(funcName)s,'
           '%(lineno)s,'
           '%(message)s, '
           '%(name)s'
)


WORDSTAT_URL = "https://api.wordstat.yandex.net/v1/dynamics"
FIXED_DELAY = 10
RETRIES = 3


def get_response():
    """
    A POST request is made with response checking and retries.
    :param retries: Number of retries
    :param kwargs: Additional arguments for requests.post()
    :return: Response data in JSON format, or None on error.
    """
    api_token = Variable.get('TOKEN_WORDSTAT')
    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "Authorization": f"Bearer {api_token}"
    }
    year = datetime.now().year - 1

    data = {
        "phrase": "мос ру",
        "devices": ["all"],
        "period": "monthly",
        "fromDate": f"{year}-01-01",
    }

    for attempt in range(RETRIES + 1):
        try:
            response = requests.post(
                url=WORDSTAT_URL,
                headers=headers,
                json=data,
                timeout=30,
            )
            print(response)
            if response.status_code == 200:
                logging.info("✅ Данные Wordstat успешно получены")
                logging.info(response.json())
                print (response)
                return response.json()

            logging.warning(
                f"HTTP {response.status_code} "
                f"(attempt {attempt + 1}/{RETRIES + 1})"
            )

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error: {e}")

        time.sleep(FIXED_DELAY)

    raise RuntimeError("❌ Wordstat request failed after retries")


def upload_to_s3(data):
    """
    Insert into s3.
    :param data:
    :return:
    """
    date_now = datetime.now().date().strftime("_%Y_%m_%d")
    hook = S3Hook(aws_conn_id="s3_connect")
    hook.load_string(
        string_data=json.dumps(data, ensure_ascii=False),
        key=f"mos_ru_dynamics_{date_now}.json",
        bucket_name="wordstat",
        replace=True,
    )


def load_from_s3():
    """Load data from s3."""
    date_now = datetime.now().date().strftime("_%Y_%m_%d")
    hook = S3Hook(aws_conn_id="s3_connect")
    content = hook.read_key(
        key=f"mos_ru_dynamics_{date_now}.json",
        bucket_name="wordstat"
    )
    rows = [json.loads(line) for line in content.splitlines()]
    print (rows)
    return rows



def get_and_upload():
    """
    Get data and insert to Clickhouse.
    :return:
    """
    data = get_response()
    upload_to_s3(data)


with DAG('wordstat_extract',
         description='select and transform data',
         schedule_interval='0 */24 * * *',
         catchup=False,
         start_date=datetime(2025, 11, 14),
         tags=['wordstat', 'etl']
         ) as dag:
    get_data = PythonOperator(task_id='start_upload',
                              python_callable=get_and_upload,
                              )
    read_s3_task = PythonOperator(
        task_id="load_from_s3",
        python_callable=load_from_s3,
    )
    start = EmptyOperator(task_id='start_workflow')

    start >> get_data >> read_s3_task

if __name__ == "__main__":
    dag.test()
