from locust import HttpUser, task, between
import json


class APITestUser(HttpUser):
    """
    Класс для нагрузочного тестирования API с помощью Locust.

    Имитирует отправку POST-запросов к эндпоинту `/predict`
    с заданным JSON, представляющим данные о распознавании автомобильного номера.

    Атрибуты: wait_time (function): Время ожидания между запросами. 
    """
    wait_time = between(0.01, 0.1)  

    @task
    def predict(self):
        payload = {
            "regno_recognize": "О041ВВ797",
            "afts_regno_ai": "О041ВВ777",
            "recognition_accuracy": 2.25,
            "afts_regno_ai_score": 0.924,
            "afts_regno_ai_char_scores": "[0.97, 0.99, 0.99]",
            "afts_regno_ai_length_scores": "[1e-9, 1e-9, 1e-9]",
            "camera_type": "Стационарная",
            "camera_class": "Астра-Трафик",
            "time_check": "2021-08-07 08:10:42",
            "direction": "1"
        }
        headers = {'Content-Type': 'application/json'}
        self.client.post("/predict", data=json.dumps(payload), headers=headers)
