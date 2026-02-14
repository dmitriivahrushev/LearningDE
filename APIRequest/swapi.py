import requests
from pathlib import Path


class APIRequester:
    """Класс для выполнения HTTP-запросов к API.
    Атрибут: base_url: Базовый URL API.
    """

    def __init__(self, base_url: str):
        self.base_url = base_url

    def get(self, tail_url: str = ""):
        """Метод выполняет GET-запрос к указанному URL.
    Параметр: tail_url: Путь, добавляемый к базовому URL.
    Возвращает: requests.Response: Ответ сервера.
    """

        url = f"{self.base_url}{tail_url}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException:
            print("Возникла ошибка при выполнении запроса")


class SWRequester(APIRequester):
    """Класс для выполнения запросов к Star Wars API.
    Наследует методы и атрибуты от APIRequester.
    """

    def get_sw_categories(self) -> dict:
        """Метод получает список категорий из Star Wars API.
        Возвращает: Список доступных категорий.
        """

        sw_category = self.get("/")
        sw_category = sw_category.json().keys()
        return sw_category

    def get_sw_info(self, sw_type: requests.Response) -> str:
        """Метод получает информации по указанной категории.
        Параметр: sw_type: Конкретная категория(films, people, planets и т.д.)
        Возвращает: Содержимое данной категории.
        """

        sw_type = self.get(f"/{sw_type}/")
        return sw_type.text


def save_sw_data():
    """Функция сохраняет данные из Star Wars API.
    Создает директорию 'data'.
    Cохраняет информацию по каждой категории в отдельный файл.
    """

    Path('data').mkdir(exist_ok=True)
    sw_category_object = SWRequester('https://swapi.dev/api')
    sw_category = sw_category_object.get_sw_categories()
    for sw_categoryes in sw_category:
        sw_categoryes_content = sw_category_object.get_sw_info(sw_categoryes)
        file_path = f"data/{sw_categoryes}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"{sw_categoryes_content}")
