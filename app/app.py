from fastapi import FastAPI
from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path)

from weatherapi.main import router as weather_router

app = FastAPI(
    title="날씨 앱 API",
    description="fastapi 라우터를 사용한 날씨 정보 제공 API",
    version="1.0.0"
)

app.include_router(
    weather_router,
    prefix="/api/weather",
    tags=["날씨"]
)

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "서버가 실행 중입니다. /docs 로 이동하세요."}