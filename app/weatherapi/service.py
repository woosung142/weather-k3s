import os
import httpx
from fastapi import HTTPException
from datetime import datetime, timedelta

KMA_API_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
KMA_SERVICE_KEY = os.getenv("KMA_SERVICE_KEY")

# API 요청 파라미터
def get_params(nx: int, ny: int):
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
    return params

async def get_current_data(nx: int, ny: int):
    params = get_params(nx, ny)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(KMA_API_URL, params=params)
            print("상태 코드:", response.status_code)
            print("응답 헤더:", response.headers)
            print("응답 내용:", response.text[:500])
            response.raise_for_status()
            
            weather_data = response.json()

            parsed = parse_items(weather_data)
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

def parse_items(data:dict):
    try:
        items = data["response"]["body"]["items"]["item"]
    except KeyError:
        raise HTTPException(status_code=500, detail="기상청 응답 구조가 올바르지 않습니다.")

    category_map = {
        "T1H": "기온(°C)",
        "REH": "습도(%)",
        "RN1": "1시간 강수량(mm)",
        "PTY": "강수형태",
        "WSD": "풍속(m/s)",
        "VEC": "풍향(°)",
    }

    rain_type_map = {
        "0": "없음",
        "1": "비",
        "2": "비/눈",
        "3": "눈",
        "5": "빗방울",
        "6": "빗방울눈날림",
        "7": "눈날림",
    }

    parsed = {}
    for item in items:
        category = item["category"]
        value = item["obsrValue"]

        if category in category_map:
            label = category_map[category]

            if category == "PTY":  # 강수형태 코드 변환
                value = rain_type_map.get(value, "알 수 없음")

            parsed[label] = float(value) if value.replace('.', '', 1).isdigit() else value

    return parsed