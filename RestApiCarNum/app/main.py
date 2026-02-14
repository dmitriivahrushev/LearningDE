from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from pick_regno import pick_regno
from catboost import CatBoostClassifier
import traceback


MODEL_PATH = "micromodel.cbm"
app = FastAPI()

@app.on_event("startup")
def load_model():
    """
    Загружаем CatBoost модель один раз при старте приложения.
    """
    model = CatBoostClassifier()
    model.load_model(MODEL_PATH)
    app.state.model = model


class InputData(BaseModel):
    """
    Модель входных данных для запроса на предсказание номера автомобиля.

    Attributes:
        regno_recognize (str): Распознанный номерной знак с камеры.
        afts_regno_ai (str): Номерной знак, предсказанный нейросетью.
        recognition_accuracy (float): Уверенность системы в правильности распознанного номера.
        afts_regno_ai_score (float): Общий confidence score от AI-модели.
        afts_regno_ai_char_scores (str): Строка с confidence по каждому символу от AI.
        afts_regno_ai_length_scores (str): Строка с вероятностями для длинны номера.
        camera_type (str): Тип камеры (например, стационарная).
        camera_class (str): Название/класс камеры.
        time_check (str): Время фиксации события.
        direction (str): Направление движения (например, "0" или "1").
    """
    regno_recognize: str
    afts_regno_ai: str
    recognition_accuracy: float
    afts_regno_ai_score: float
    afts_regno_ai_char_scores: str
    afts_regno_ai_length_scores: str
    camera_type: str
    camera_class: str
    time_check: str
    direction: str

@app.post('/predict')
def predict(data: InputData, request: Request):
    """
    Эндпоинт для получения предсказания по распознанному номерному знаку.

    Принимает JSON с параметрами о распознавании номера от камеры и AI-модели.
    Возвращает лучший предсказанный номерной знак на основе обученной CatBoost модели.

    Args: data (InputData): Входные параметры, описывающие событие распознавания.
    Returns: dict: Словарь с ключом 'prediction', содержащим список с предсказанным номером.
    Raises: HTTPException: Если при обработке возникает исключение, возвращается статус 500.
    """
    model = request.app.state.model
    try:
        result = pick_regno(   
        data.regno_recognize,
        data.afts_regno_ai,
        data.recognition_accuracy,
        data.afts_regno_ai_score,
        data.afts_regno_ai_char_scores,
        data.afts_regno_ai_length_scores,
        data.camera_type,
        data.camera_class,
        data.time_check,
        data.direction,
        model
        )
        return {'prediction': result.tolist()}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


