import os
import httpx
from fastapi import APIRouter, HTTPException, Query
from dotenv import load_dotenv
from datetime import datetime, timedelta

#load_dotenv()

router = APIRouter()

KMA_API_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
KMA_SERVICE_KEY = os.getenv("KMA_SERVICE_KEY")

@router.get("/current")
async def get_current_weather(
    nx: int = Query(60, description="예보지점 X 좌표"),
    ny: int = Query(127, description="예보지점 Y 좌표")
):
    now = datetime.now()

    target_time = now - timedelta(hours=1)
    base_date = target_time.strftime('%Y%m%d')
    base_time = target_time.strftime('%H30')

    params = {
        "serviceKey": KMA_SERVICE_KEY,
        "pageNo": 1,
        "numOfRows": 10,
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(KMA_API_URL, params=params)
            response.raise_for_status()
            
            weather_data = response.json()
            
            return weather_data

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")
