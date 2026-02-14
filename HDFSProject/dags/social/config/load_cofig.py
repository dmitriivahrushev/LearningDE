import os
import json
from dataclasses import dataclass


@dataclass
class Config():
    """
    Класс конфигурации.
    master: Режим выполнения (local, yarn)
    load_date: Дата за которую загружаем датасет.
    Параметры можно указать в ./config/configs.json
    """
    master: str
    load_date: str

    def _load_config_files() -> dict:
        """
        Метод:
        Загружает конфигурационные файлы из директории
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
                continue

        return configs

    @staticmethod
    def unpack_config():
        """
        Метод:
        Возвращает конфигурационные параметры указанные в configs.json
        """
        configs = Config._load_config_files()
        config_dict = list(configs.values())[0]

        return Config(
            master=config_dict["spark_session"]["master"],
            load_date=config_dict["load_date"]
        )
