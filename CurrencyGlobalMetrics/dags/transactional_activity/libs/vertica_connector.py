import logging
from contextlib import contextmanager
from typing import Optional, Tuple, List, Any
import vertica_python

logger = logging.getLogger(__name__)


class VerticaManager:
    """
    Клиент для подключения к Vertica.
    Использование:
        Импортируем конфиг from configs.load_config import Config
                           configs = Config.unpack_config()
        Открываем менеджер контекста, в который передаем запрос.
        with VerticaManager(configs) as db:
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
            conn_info = {
                'host': self.config.vertica_host,
                'port': self.config.vertica_port,
                'user': self.config.vertica_username,
                'password': self.config.vertica_password,
                'database': self.config.vertica_database,
                'use_prepared_statements': True,
                'tlsmode': 'disable',
            }
            self.connection = vertica_python.connect(**conn_info)
            return True
        except Exception as e:
            logger.error(f'Ошибка подключения к Базе данных Vertica!\n{e}')
            raise

    @contextmanager
    def get_cursor(self):
        if not self.connection:
            self.get_connect()

        cursor = self.connection.cursor() 
        try:
            yield cursor
        except Exception as e:
            logger.error(f'Ошибка выполнения запроса в Vertica!\n{e}')
            raise
        finally:
            cursor.close()

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Any]:
        """
        Выполняет SQL-запрос и возвращает результат.
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def execute_non_query(self, query: str, params: Optional[Tuple] = None):
        """
        Выполняет SQL-запрос, который не возвращает результат (INSERT, UPDATE, DELETE).
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            self.connection.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
           self.connection.close()
