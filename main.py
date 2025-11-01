from datetime import datetime, timedelta
import os
import boto3
import dotenv
import requests

dotenv.load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")

url = "https://api.wordstat.yandex.net/v1/dynamics"


headers = {
    "Content-Type": "application/json;charset=utf-8",
    "Authorization": f"Bearer {access_token}"
}
year = datetime.now().year


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


def save_s3(data):
    """
    Parse response.
    :param weather:
    :return:
    """

    json_data = data
    pfrase = data['requestPhrase']
    dynamics = data['dynamics']
    # minioClient = Minio('minio:9000',
    #                     access_key='weak_access_key',
    #                     secret_key='weak_secret_key',
    #                     secure=False)
    # minioClient.list_buckets()


if __name__ == '__main__':
    save_s3(get_response())
