import datetime
import os
import time
import dotenv
import logging
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")
schema_name = os.getenv("DB_SCHEMA")
table_name = os.getenv("DB_TABLE_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASS")

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
# access_key_id = os.getenv("KEY_ID")
# secret_access_key = os.getenv("ACCESS_KEY")

url = "https://api.wordstat.yandex.net/v1/"

endpoints = ("dynamics",
             "regions",
             "topRequests",
             "getRegionsTree"
             )

headers = {
    "Content-Type": "application/json;charset=utf-8",
    "Authorization": f"Bearer {access_token}"
}
year = datetime.datetime.now().year - 1
date_now = datetime.datetime.now().date().strftime("_%Y_%m_%d")

data_dynamics = {
    "phrase": "",
    "devices": ["all"],
    "period": "monthly",
    "fromDate": f"{year}-01-01"
}

data_regions = {
    "phrase": "",
    "devices": ["all"],
    "regionType": "regions"
}

companies = ['Альфа-Лизинг',
             'Европлан',
             'РЕСО-Лизинг',
             'ПСБ Лизинг',
             'Совкомбанк Лизинг',
             'Росагролизинг',
             'Каркаде',
             'Интерлизинг',
             'СберЛизинг',
             'Эволюция',
             'ВТБ Лизинг',
             'Газпромбанк Автолизинг',
             'Ликонс',
             'Балтийский Лизинг',
             'Дельта Лизинг',
             'Флит Автолизинг',
             'ГТЛК',
             'ЧелИндЛизинг',
             'ПР-Лизинг',
             'Элемент Лизинг',
             'ТрансФин-М',
             'КАМАЗ-ЛИЗИНГ',
             'ФЛИТ',
             'Альянс-Лизинг',
             'БелФин',
             'ТаймЛизинг',
             'Росбанк Лизинг',
             'Бизнес Кар Лизинг',
             'ТЕХНО Лизинг',
             'Директ Лизинг',
             'Стоун-XXI',
             'Реалист Лизинг',
             'ЯрКамп-Лизинг',
             'МК Лизинг',
             'Аренза',
             'Фера',
             'Столичный лизинг',
             '+7 ИНВЕСТ',
             'ГКР Лизинг',
             'МСБ Лизинг',
             'GLS Лизинг',
             'АзурДрайв',
             'Аспект Лизинг',
             'АТБ-Лизинг',
             'Инавтотрак Лизинг',
             'Контрол Лизинг',
             'Лизинг-Трейд',
             'Лизинг Москоу',
             'Роделен',
             'Уралпромлизинг',
             'Южноуральский лизинговый центр',
             'Сан Финанс',
             'СГБ-Лизинг',
             'СОБИ ЛИЗИНГ',
             'Пруссия',
             'Универсальная лизинговая компания',
             'Восток-Лизинг',
             'Мэйджор Лизинг',
             'Т-Лизинг',
             'УралБизнесЛизинг',
             'КузбассФинансЛизинг',
             'ЛК ПроДвижение',
             'Дойче Финанс Восток',
             'РСХБ Лизинг',
             'Флит Финанс',
             'Сибирская лизинговая компания',
             'Лизинг-Медицина',
             'ТАЛК',
             'ИНВЕСТ-лизинг',
             'ФИНСМАРТ',
             'ТСС-Лизинг',
             'Ак Барс Лизинг',
             'ЭкономЛизинг',
             'ФинТех Лизинг',
             'Лизинговые Решения',
             'Абсолют Лизинг',
             'Шелковый путь',
             'Транслизинг',
             'Компания «Финансовый Партнер»',
             'Озон Лизинг',
             'Эксперт лизинг',
             'ФКМ Лизинг',
             'Арктика',
             'Адванстрак',
             'Межрегиональная инвестиционная компания',
             'Азия корпорейшн',
             'Транспортные решения',
             'Технологии Лизинга и Финансы',
             'Лизинговая компания малого бизнеса Республики Татарстан',
             'РАФТ ЛИЗИНГ',
             'АС Финанс',
             'КМ-Финанс',
             'ПТК-лизинг',
             'МАШПРОМЛИЗИНГ',
             'Лизинговое агентство',
             'Сеспель-Финанс',
             'ГЕН ЛИЗИНГ',
             'Оренбургская губернская лизинговая компания',
             'СОЛИД СпецАвтоТехЛизинг',
             'А-Лизинг',
             'КВАЗАР лизинг',
             'Пензенская лизинговая компания',
             'Горизонт Лизинг',
             'Аквилон-Лизинг',
             'Объединенная лизинговая компания',
             'ВЕЛКОР',
             'Ленобллизинг',
             'РЕКОРД ЛИЗИНГ',
             'АСТ Лизинг',
             'Лентранслизинг',
             'Независимая лизинговая компания',
             'Петербургснаб',
             'БЭЛТИ-ГРАНД',
             'Межрегиональная лизинговая компания',
             'Русавтолизинг',
             'Пионер-Лизинг']


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


def get_companies_df(endpoint):
    """
    Create DataFrame.
    :param endpoint:
    :return:
    """
    all_df = []
    for company in companies:
        if endpoint == "dynamics":
            data_dynamics['phrase'] = company
            json = data_dynamics
        elif endpoint == "regions":
            data_regions['phrase'] = company
            json = data_regions

        response = get_response(
            url=(url + endpoint),
            headers=headers,
            json=json
        )
        # response2 = get_response(
        #     url=(url + endpoints[3]),
        #     headers=headers,
        # )
        if isinstance(response, dict):
            data = response

        df = pd.DataFrame(data[endpoint])
        df['requestPhrase'] = data['requestPhrase']
        all_df.append(df)
    result = pd.concat(all_df, ignore_index=True)
    return result


def write_profiles_to_csv(df, flag=True):
    """
    Запись информации в файл из DataFrame.
    :param df, flag:
    :return:
    """
    #path = datetime.date.today.__str__().replace("-", "_")
    filename = f"leasing_{date_now}.csv"
    df.to_csv(
        f"{filename}", mode="a", sep=";", header=flag, index=False,
        encoding="utf-16"
    )
    return filename


def load_db(filename):
    """
    Загрузка в stage слой.
    :param path:
    :return:
    """

    database_uri = (
        f"postgresql://{user}:{password}@{host}/{database}")

    engine = create_engine(database_uri)

    try:
        df = pd.read_csv(
            filename,
            encoding='utf-16',
            delimiter=';',
            header=0,
            engine='python',

        )
    except Exception as e:
        print(f"Ошибка при загрузке CSV: {e}")

    # df.drop_duplicates(subset=['id'], keep='first', inplace=True)
    # if 'is_check' in df.columns:
    #     df['is_check'] = df['is_check'].astype(bool)
    df.insert(0, 'date_load', datetime.datetime.now().date().strftime(
        "%Y-%m-%d"))

    with engine.begin() as connection:
        df.to_sql(
            table_name,
            connection,
            schema=schema_name,
            if_exists='append',
            index=False
        )


if __name__ == '__main__':
    # load_db(write_profiles_to_csv(get_companies_df(endpoints[0])))
    load_db("leasing__2026_04_04.csv")
