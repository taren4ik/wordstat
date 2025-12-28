from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import os
import time
import json
import dotenv
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


# access_key_id = os.getenv("KEY_ID")
# secret_access_key = os.getenv("ACCESS_KEY")



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
    year = datetime.now().year

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
                return

            logging.warning(
                f"HTTP {response.status_code} "
                f"(attempt {attempt + 1}/{RETRIES + 1})"
            )

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error: {e}")

        time.sleep(FIXED_DELAY)

    raise RuntimeError("❌ Wordstat request failed after retries")


# def upload_s3(data, endpoint):
#     """
#     Upload to s3 file.
#     :param data:
#     :return:
#     """
#     try:
#         s3 = boto3.client(
#             's3',
#             endpoint_url='http://127.0.0.1:9100',
#             aws_access_key_id=access_key_id,
#             aws_secret_access_key=secret_access_key,
#             region_name='us-east-1'
#         )
#         buffer = json.dumps(data, ensure_ascii=False).encode("utf-8")
#         s3.put_object(
#             Body=buffer,
#             Bucket="wordstat",
#             Key=f"mos_ru_{endpoint}_{date_now}.json",
#         )
#         logging.info("✅ Upload successful from endpoint")
#
#     except FileNotFoundError:
#         logging.error("❌ File not found")
#     except NoCredentialsError:
#         logging.error("❌ Invalid credentials")
#     except ClientError as e:
#         logging.error(f"❌ AWS error: {e}")

#
# def start_upload():
#     upload_s3(get_response(
#                 url=(url + endpoint),
#                 headers=headers,
#                 json=data), endpoint
#             )

with DAG('wordstat_extract',
         description='select and transform data',
         schedule_interval='0 */24 * * *',
         catchup=False,
         start_date=datetime(2025, 11, 14),
         tags=['wordstat', 'etl']
         ) as dag:
    get_data = PythonOperator(task_id='start_upload',
                              python_callable=get_response,
                              )

    start = EmptyOperator(task_id='start_workflow')

    start >> get_data

if __name__ == "__main__":
    dag.test()
