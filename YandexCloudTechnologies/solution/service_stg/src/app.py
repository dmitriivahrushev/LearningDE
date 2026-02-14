import logging
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor
from stg_loader.repository.stg_repository import StgRepository

app = Flask(__name__)


@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)
    config = AppConfig()

    proc = StgMessageProcessor(
        consumer=config.kafka_consumer(),
        producer=config.kafka_producer(),
        redis_client=config.redis_client(),
        stg_repository=StgRepository(config.pg_warehouse_db()),
        logger=app.logger,
        batch_size=100
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
