from datetime import datetime
import os
import time
import json
import boto3
import dotenv
import logging
import requests
from botocore.exceptions import NoCredentialsError, ClientError

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
dotenv.load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")
access_key_id = os.getenv("KEY_ID")
secret_access_key = os.getenv("ACCESS_KEY")

url = "https://api.wordstat.yandex.net/v1/"

endpoints = ["dynamics", "topRequests"]

headers = {
    "Content-Type": "application/json;charset=utf-8",
    "Authorization": f"Bearer {access_token}"
}
year = datetime.now().year
date_now = datetime.now().date().strftime("_%Y_%m_%d")

data_dynamics = {
    "phrase": "мос ру",
    "devices": ["all"],
    "period": "monthly",
    "fromDate": f"{year}-01-01",
}

data_top = {
    "phrase": "мос ру",
    "devices": ["all"]
}

def get_response(retries=3, **kwargs):
    """
    A POST request is made with response checking and retries.
    :param retries: Number of retries
    :param kwargs: Additional arguments for requests.post()
    :return: Response data in JSON format, or None on error.
    """
    FIXED_DELAY = 10

    for attempt in range(retries + 1):
        try:
            response = requests.post(**kwargs)
            if response.status_code == 200:
                logging.info(f'Данные получены, код {response.status_code}')
                return response.json()
            else:
                logging.warning(
                    f'Ошибка {response.status_code} ('
                    f'попытка {attempt + 1}/{retries + 1})')

        except requests.exceptions.RequestException as e:
            logging.error(
                f'Сетевая ошибка: {e} (попытка {attempt + 1}/{retries + 1})')

        if attempt < retries:
            logging.info(
                f'Ожидание {FIXED_DELAY} секунд перед повторной попыткой...')
            time.sleep(FIXED_DELAY)
        else:
            logging.error(
                f'Все попытки исчерпаны. Последний статус:'
                f' {response.status_code}')


def upload_s3(data, endpoint):
    """
    Upload to s3 file.
    :param data:
    :return:
    """
    try:
        s3 = boto3.client(
            's3',
            endpoint_url='http://127.0.0.1:9100',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name='us-east-1'
        )
        buffer = json.dumps(data, ensure_ascii=False).encode("utf-8")
        s3.put_object(
            Body=buffer,
            Bucket="wordstat",
            Key=f"mos_ru_{endpoint}_{date_now}.json",
        )
        logging.info("✅ Upload successful from endpoint")

    except FileNotFoundError:
        logging.error("❌ File not found")
    except NoCredentialsError:
        logging.error("❌ Invalid credentials")
    except ClientError as e:
        logging.error(f"❌ AWS error: {e}")

    # json_data = data
    # pfrase = data['requestPhrase']
    # dynamics = data['dynamics']


if __name__ == '__main__':
    for endpoint in endpoints:
        if endpoint == "dynamics":
            upload_s3(get_response(
                url=(url + endpoint),
                headers=headers,
                json=data_dynamics), endpoint
            )
        else:
            upload_s3(get_response(
                url=(url + endpoint),
                headers=headers,
                json=data_top), endpoint
            )
