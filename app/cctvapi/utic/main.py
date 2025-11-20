# /home/dubuntu/weather-k3s/app/cctvapi/main.py

from fastapi import FastAPI
from datetime import datetime

app = FastAPI()
@app.get("/")
def read_root():
    return {"status": "ok", "message": "CCTV API is ready."}
@app.get("/cctv/{camera_id}")
def get_cctv_status(camera_id: int):
    """
    특정 ID의 CCTV 상태 정보를 반환
    """
    
    return {
        "camera_id": camera_id,
        "status": "Online",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "note": "Step 1 complete: Basic routing works."
    }