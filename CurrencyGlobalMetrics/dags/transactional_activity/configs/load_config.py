import os
import json
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Config:
    """
    Класс конфигурации.
    pg_database: Имя Базы данных Postgres.
    pg_host: Хост Postgres.
    pg_port: Порт Postgres.
    pg_username: Имя пользователя Postgres.
    pg_password: Пароль пользователя Postgres.
    batch_size: Размер батча.
    pg_table_transactions_schema_name: Имя схемы таблицы транзакций.
    pg_table_transactions_table_name: Имя таблицы транзакций.
    pg_table_transactions_dt_load_column: Дата выполнения транзакций.
    pg_table_transactions_pk_column: Поле идентификатор записи таблицы транзакций.
    pg_table_currencies_schema_name: Имя схемы справочника обновления валют.
    pg_table_currencies_table_name: Имя таблицы справочника обновления валют.
    pg_table_currencies_dt_load_column: Поле обновления курса валют.

    vertica_database: Имя Базы данных Vertica.
    vertica_host: Хост Vertica.
    vertica_port: Порт Vertica.
    vertica_username: Имя пользователя Vertica.
    vertica_password: Пароль пользователя Vertica.
    vertica_table_transactions_schema_name: Имя схемы таблицы транзакций.
    vertica_table_transactions_table_name: Имя таблицы транзакций.
    vertica_table_currencies_schema_name: Имя схемы справочника обновления валют.
    vertica_table_currencies_table_name: Имя таблицы справочника обновления валют.

    vertica_table_global_metrics_table_name: Имя схемы DWH.
    vertica_table_global_metrics_schema_name: Название таблицы - витрины global_metrics.
    """
    pg_database: str
    pg_host: str
    pg_port: int
    pg_username: str
    pg_password: str
    batch_size: int

    pg_table_transactions_schema_name: str
    pg_table_transactions_table_name: str
    pg_table_transactions_dt_load_column: str
    pg_table_transactions_pk_column: str

    pg_table_currencies_schema_name: str
    pg_table_currencies_table_name: str
    pg_table_currencies_dt_load_column: str

    vertica_database: str
    vertica_host: str
    vertica_port: int
    vertica_username: str
    vertica_password: str

    vertica_table_transactions_schema_name: str
    vertica_table_transactions_table_name: str

    vertica_table_currencies_schema_name: str
    vertica_table_currencies_table_name: str

    vertica_table_global_metrics_schema_name: str
    vertica_table_global_metrics_table_name: str

    def _load_config_files() -> dict:
        """
        Метод:
        Загружает конфигурационные файлы из директории.
        """

        configs = {}
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        for filename in sorted(os.listdir(BASE_DIR)):
            filepath = os.path.join(BASE_DIR, filename)

            try:
                if filename.endswith('.json'):
                    with open(filepath, 'r') as f:
                        config = json.load(f)
                        configs[filename[:-5]] = config
    
            except Exception as e:
                logger.error(f"Не удалось загрузить конфигурационный файл: {filename}: {str(e)}", exc_info=True)
                continue

        return configs

    @staticmethod
    def unpack_config():
        """
        Метод:
        Возвращает конфигурационные параметры указанные в виде объекта Config.
        """

        configs = Config._load_config_files()
        config_dict = list(configs.values())[0] 

        batch_size = config_dict["postgres_conn"]["batch_size"]
        pg_database = config_dict["postgres_conn"]["database"]
        pg_host = config_dict["postgres_conn"]["host"]
        pg_port = config_dict["postgres_conn"]["port"]
        pg_username = config_dict["postgres_conn"]["username"]
        pg_password = config_dict["postgres_conn"]["password"]

        pg_table_transactions_schema_name = config_dict["postgres_conn"]["pg_table_transactions"]["schema_name"]
        pg_table_transactions_table_name = config_dict["postgres_conn"]["pg_table_transactions"]["table_name"]
        pg_table_transactions_dt_load_column = config_dict["postgres_conn"]["pg_table_transactions"]["dt_load_column"]
        pg_table_transactions_pk_column = config_dict["postgres_conn"]["pg_table_transactions"]["pk_column"]
        pg_table_currencies_schema_name = config_dict["postgres_conn"]["pg_table_currencies"]["schema_name"]
        pg_table_currencies_table_name = config_dict["postgres_conn"]["pg_table_currencies"]["table_name"]
        pg_table_currencies_dt_load_column = config_dict["postgres_conn"]["pg_table_currencies"]["dt_load_column"]

        vertica_database = config_dict["vertica_conn"]["database"]
        vertica_host = config_dict["vertica_conn"]["host"]
        vertica_port = config_dict["vertica_conn"]["port"]
        vertica_username = config_dict["vertica_conn"]["username"]
        vertica_password = config_dict["vertica_conn"]["password"]

        vertica_table_transactions_schema_name = config_dict["vertica_conn"]["vertica_table_transactions"]["schema_name"]
        vertica_table_transactions_table_name = config_dict["vertica_conn"]["vertica_table_transactions"]["table_name"]
        vertica_table_currencies_schema_name = config_dict["vertica_conn"]["vertica_table_currencies"]["schema_name"]
        vertica_table_currencies_table_name = config_dict["vertica_conn"]["vertica_table_currencies"]["table_name"]

        vertica_table_global_metrics_schema_name = config_dict["vertica_conn"]["vertica_table_global_metrics"]["schema_name"]
        vertica_table_global_metrics_table_name = config_dict["vertica_conn"]["vertica_table_global_metrics"]["table_name"]

        return Config(
            pg_database,
            pg_host,
            pg_port,
            pg_username,
            pg_password,
            batch_size,
            pg_table_transactions_schema_name,
            pg_table_transactions_table_name,
            pg_table_transactions_dt_load_column,
            pg_table_transactions_pk_column,
            pg_table_currencies_schema_name,
            pg_table_currencies_table_name,
            pg_table_currencies_dt_load_column,
            vertica_database,
            vertica_host,
            vertica_port,
            vertica_username,
            vertica_password,
            vertica_table_transactions_schema_name,
            vertica_table_transactions_table_name,
            vertica_table_currencies_schema_name,
            vertica_table_currencies_table_name,
            vertica_table_global_metrics_schema_name,
            vertica_table_global_metrics_table_name
        )
