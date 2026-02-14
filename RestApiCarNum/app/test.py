import pandas as pd
import requests


"""
Проверка на тестовых данных.
"""
df = pd.read_csv("test_data.csv")

for _, row in df.iterrows():
    payload = {
    "regno_recognize": row["regno_recognize"],
    "afts_regno_ai": row["afts_regno_ai"],
    "recognition_accuracy": row["recognition_accuracy"],
    "afts_regno_ai_score": row["afts_regno_ai_score"],
    "afts_regno_ai_char_scores": row["afts_regno_ai_char_scores"],
    "afts_regno_ai_length_scores": row["afts_regno_ai_length_scores"],
    "camera_type": row["camera_type"],
    "camera_class": row["camera_class"],
    "time_check": row["time_check"],
    "direction": str(row["direction"]),
}
    r = requests.post("http://localhost:8000/predict", json=payload)
    print(f"{row['regno_recognize']} → {r.json()}")
