import logging
import psycopg2
from contextlib import contextmanager
from typing import Optional, Dict, Tuple, List

logger = logging.getLogger(__name__)


class PostgresManager:
    """
    Клиент для подключения к Postgres.
    Использование:
        Импортируем конфиг from configs.load_config import Config
                           configs = Config.unpack_config()
        Открываем менеджер контекста, в который передаем запрос.
        with PostgresManager(configs) as db:
            query = db.execute_query("SELECT * FROM schema.table_name;")
    """

    def __init__(self, config):
        self.config = config
        self.connection = None

    def __enter__(self):
        self.get_connect()
        return self

    def get_connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(
                dbname = self.config.pg_database,
                user = self.config.pg_username,
                password = self.config.pg_password,
                host = self.config.pg_host,
                port = self.config.pg_port
            )
            return True
        except Exception as e:
            logger.error(f'Ошибка подключения к Базе данных!\n{e}')
            raise

    @contextmanager
    def get_cursor(self):
        if not self.connection:
            self.get_connect()

        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            logger.error(f'Отмена транзакции!\n{e}')
            raise
        finally:
            cursor.close()

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
           self.connection.close()
