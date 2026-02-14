import json
from datetime import datetime
from logging import Logger
from typing import Any, Dict, List

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        redis_client: RedisClient,
        stg_repository: StgRepository,
        batch_size: int,
        logger: Logger,
    ) -> None:

        self.consumer = consumer
        self.producer = producer
        self.redis = redis_client
        self.stg_repository = stg_repository
        self.batch_size = batch_size
        self.logger = logger

    def run(self) -> None:
        self.logger.info(f"{datetime.utcnow()}: START")
        processed_count = 0

        for _ in range(self.batch_size):
            msg = self.consumer.consume()
            if not msg:
                self.logger.info("Сообщений в Kafka больше нет, завершение обработки")
                break

            try:
                self._process_message(msg)
                processed_count += 1
            except Exception as e:
                self.logger.error(f"Ошибка при обработке сообщения {msg.get('object_id')}: {e}")
                continue

        self.logger.info(f"{datetime.utcnow()}: FINISH. Обработано {processed_count} сообщений")

    def _process_message(self, msg: Dict[str, Any]) -> None:
        payload = msg["payload"]
        self.stg_repository.order_events_insert(
            object_id=msg["object_id"],
            object_type=msg["object_type"],
            sent_dttm=datetime.fromisoformat(msg["sent_dttm"].replace("Z", "+00:00")),
            payload=json.dumps(payload),
        )

        user = self._get_user(payload)
        restaurant, menu = self._get_restaurant(payload)
        products = self._process_products(payload.get("order_items", []), menu)

        output_msg = {
            "object_id": msg["object_id"],
            "object_type": msg["object_type"],
            "payload": {
                "id": msg["object_id"],
                "date": payload.get("date", ""),
                "cost": payload.get("cost", 0),
                "payment": payload.get("payment", 0),
                "status": payload.get("final_status", ""),
                "restaurant": {"id": restaurant.get("id", ""), "name": restaurant.get("name", "")},
                "user": {"id": user.get("id", ""), "name": user.get("name", "")},
                "products": products,
            },
        }

        self.producer.produce(output_msg)
        self.logger.info(f"Сообщение успешно отправлено: {msg['object_id']}")

    def _get_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        user_id = payload["user"]["id"]
        user = self.redis.get(user_id) or {}
        user["id"] = user_id
        return user

    def _get_restaurant(self, payload: Dict[str, Any]) -> (Dict[str, Any], List[Dict[str, Any]]):
        restaurant_id = payload["restaurant"]["id"]
        restaurant = self.redis.get(restaurant_id) or {}
        restaurant["id"] = restaurant_id
        menu = restaurant.get("menu", [])
        return restaurant, menu

    def _process_products(self, order_items: List[Dict[str, Any]], menu: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        products_result = []
        for item in order_items:
            category = next((m.get("category") for m in menu if m.get("_id") == item["id"]), None)
            products_result.append(
                {
                    "id": item["id"],
                    "name": item["name"],
                    "price": item["price"],
                    "quantity": item["quantity"],
                    "category": category,
                }
            )
        return products_result
