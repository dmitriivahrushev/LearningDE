import json
from datetime import datetime
from logging import Logger
from collections import defaultdict

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger) -> None:

        self._kafka_consumer = kafka_consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        processed_messages = 0
        timeout: float = 3.0 

        while processed_messages < self._batch_size:
            raw_msg = self._kafka_consumer.consume(timeout=timeout)
            self._logger.info(f"{datetime.utcnow()}: INPUT MESSAGE: {raw_msg}")

            if not raw_msg:
                continue

            if isinstance(raw_msg, str):
                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    self._logger.error(f"Не удалось распарсить JSON: {raw_msg}")
                    continue
            else:
                msg = raw_msg

            cdm_events = [m for m in msg if m.get('message_type') == 'cdm_event']
            if not cdm_events:
                continue

            user_category_orders = defaultdict(set)
            user_product_orders = defaultdict(set)

            for event in cdm_events:
                payload = event.get('payload', {})
                user_id = payload.get('user_id')
                category_id = payload.get('category_id')
                category_name = payload.get('category_name')
                product_id = payload.get('product_id')
                product_name = payload.get('product_name')
                order_id = payload.get('order_id')

                if user_id and category_id and order_id:
                    user_category_orders[(user_id, category_id, category_name)].add(order_id)

                if user_id and product_id and order_id:
                    user_product_orders[(user_id, product_id, product_name)].add(order_id)

            for (user_id, category_id, category_name), orders in user_category_orders.items():
                self._cdm_repository.user_category_counters_upsert(
                    user_id=user_id,
                    category_id=category_id,
                    category_name=category_name,
                    order_cnt=len(orders)
                )

            for (user_id, product_id, product_name), orders in user_product_orders.items():
                self._cdm_repository.user_product_counters_upsert(
                    user_id=user_id,
                    product_id=product_id,
                    product_name=product_name,
                    order_cnt=len(orders)
                )

            processed_messages += 1

        self._logger.info(f"{datetime.utcnow()}: FINISH. Обработано сообщений: {processed_messages}")
