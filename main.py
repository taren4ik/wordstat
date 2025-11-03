from datetime import datetime
import os
import json
import boto3
import dotenv

import requests
from botocore.exceptions import NoCredentialsError, ClientError

dotenv.load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")
access_key_id = os.getenv("KEY_ID")
secret_access_key = os.getenv("ACCESS_KEY")

url = "https://api.wordstat.yandex.net/v1/dynamics"


headers = {
    "Content-Type": "application/json;charset=utf-8",
    "Authorization": f"Bearer {access_token}"
}
year = datetime.now().year
date_now = datetime.now().date().strftime("_%Y_%m_%d")

data = {
    "phrase": "мос ру",
    "devices": ["all"],
    "period": "monthly",
    "fromDate": f"{year}-01-01",
}


def get_response(**kwargs):
    """
    Get and check response.
    :return:
    """
    response = requests.post(
        url,
        headers=headers,
        json=data
    )
    if response.status_code == 200:
        print(f'Данные получены, {response.status_code}')
    else:
        print(f'Ошибка: {response.status_code}')
    return response.json()


def upload_s3(data):
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
            Body = buffer,
            Bucket = "wordstat",
            Key = f"mos_ru_{date_now}.json",
        )
        print("✅ Upload successful")

    except FileNotFoundError:
        print("❌ File not found")
    except NoCredentialsError:
        print("❌ Invalid credentials")
    except ClientError as e:
        print(f"❌ AWS error: {e}")

    # json_data = data
    # pfrase = data['requestPhrase']
    # dynamics = data['dynamics']


if __name__ == '__main__':
    upload_s3(get_response())
