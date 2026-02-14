"""
Модуль для тестирования.
"""
from utils.load_stg_to_vertica import load_staging_transaction
from utils.load_stg_to_vertica import load_staging_currencies
from utils.load_global_metrics import load_global_metricts
from configs.load_config import Config


def main():
    configs = Config.unpack_config()
    dt_load_inteval = '2022-10-05'
    load_staging_transaction(configs, dt_load_inteval)
    load_staging_currencies(configs, dt_load_inteval)
    load_global_metricts(configs, dt_load_inteval)


if __name__ == "__main__":
    main()
