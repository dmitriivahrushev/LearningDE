import uuid
import json
from datetime import datetime
from logging import Logger

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:

        self._kafka_consumer = kafka_consumer
        self._kafka_producer = kafka_producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START processing batch")

        processed_messages = 0
        timeout = 3.0

        while processed_messages < self._batch_size:
            msg = self._kafka_consumer.consume(timeout=timeout)
            if msg is None:
                self._logger.info("Нет новых сообщений для обработки")
                break

            if 'object_type' not in msg:
                continue
            if msg['object_type'] != 'order':
                continue

            load_dt = datetime.utcnow()
            load_src = "orders_backend"

            try:
                order_id = msg['payload']['id']
                h_order_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(order_id))
                order_dt = msg['payload']['date']
                order_cost = msg['payload']['cost']
                order_payment = msg['payload']['payment']
                order_status = msg['payload']['status']

                self._dds_repository.order_upsert(
                    h_order_pk, order_id, order_dt,
                    order_cost, order_payment, order_status,
                    load_dt, load_src
                )

                user_id = msg['payload']['user']['id']
                h_user_pk = uuid.uuid3(uuid.NAMESPACE_X500, user_id)
                username = msg['payload']['user']['name']
                userlogin = msg['payload']['user'].get('login', username)

                self._dds_repository.user_upsert(
                    h_user_pk, user_id, username, userlogin,
                    load_dt, load_src
                )

                restaurant_id = msg['payload']['restaurant']['id']
                h_restaurant_pk = uuid.uuid3(uuid.NAMESPACE_X500, restaurant_id)
                restaurant_name = msg['payload']['restaurant']['name']

                self._dds_repository.restaurant_upsert(
                    h_restaurant_pk, restaurant_id, restaurant_name,
                    load_dt, load_src
                )

                products = msg['payload']['products']
                categories = {p['category'] for p in products}

                for category_name in categories:
                    h_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, category_name)
                    self._dds_repository.category_upsert(
                        h_category_pk, category_name, load_dt, load_src
                    )

                for product in products:
                    product_id = product['_id']
                    product_name = product['name']
                    category_name = product['category']

                    h_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, product_id)
                    h_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, category_name)

                    self._dds_repository.product_upsert(
                        h_product_pk, product_id, product_name, load_dt, load_src
                    )

                    hk_product_category_pk = uuid.uuid3(
                        uuid.NAMESPACE_X500, f"{h_product_pk}/{h_category_pk}"
                    )
                    self._dds_repository.l_product_category_upsert(
                        hk_product_category_pk, h_product_pk, h_category_pk,
                        load_dt, load_src
                    )

                    hk_product_restaurant_pk = uuid.uuid3(
                        uuid.NAMESPACE_X500, f"{h_product_pk}/{h_restaurant_pk}"
                    )
                    self._dds_repository.l_product_restaurant_upsert(
                        hk_product_restaurant_pk, h_product_pk, h_restaurant_pk,
                        load_dt, load_src
                    )

                    hk_order_product_pk = uuid.uuid3(
                        uuid.NAMESPACE_X500, f"{h_order_pk}/{h_product_pk}"
                    )
                    self._dds_repository.l_order_product_upsert(
                        hk_order_product_pk, h_order_pk, h_product_pk,
                        load_dt, load_src
                    )

                hk_order_user_pk = uuid.uuid3(
                    uuid.NAMESPACE_X500, f"{h_order_pk}/{h_user_pk}"
                )
                self._dds_repository.l_order_user_upsert(
                    hk_order_user_pk, h_order_pk, h_user_pk,
                    load_dt, load_src
                )

                cdm_msgs = []
                if order_status == 'CLOSED':
                    for product in products:
                        product_id = product['_id']
                        product_name = product['name']
                        category_name = product['category']
                        h_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, product_id)
                        h_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, category_name)

                        cdm_msgs.append({
                            "message_id": str(uuid.uuid4()),
                            "message_type": "cdm_event",
                            "payload": {
                                "order_id": str(h_order_pk),
                                "user_id": str(h_user_pk),
                                "product_id": str(h_product_pk),
                                "product_name": product_name,
                                "category_id": str(h_category_pk),
                                "category_name": category_name
                            }
                        })

                if cdm_msgs:
                    msg_json = json.dumps(cdm_msgs, ensure_ascii=False)
                    self._logger.info(f"{datetime.utcnow()} : CDM MESSAGE : {msg_json}")
                    self._kafka_producer.produce(msg_json)

                processed_messages += 1

            except Exception as e:
                self._logger.error(f"Ошибка обработки сообщения: {e}")
                continue

        self._logger.info(f"{datetime.utcnow()}: FINISH processing batch, messages processed: {processed_messages}")
