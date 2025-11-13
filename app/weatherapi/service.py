import os
import logging
import httpx
from fastapi import HTTPException
from datetime import datetime, timedelta
from shared.redis.client import redis

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

KMA_API_BASE_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0"
KMA_SERVICE_KEY = os.getenv("KMA_SERVICE_KEY")

#------------------------------------------------------
# 초단기실황조회(현재날씨)
#------------------------------------------------------
CASHE_EXPIRE = 300  # 5분

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
    cache_key = f"weather:{nx}:{ny}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용")
        return eval(cached)
    logging.info("기상청 API에서 날씨 데이터 조회")

    params = get_params(nx, ny)
    try:
        async with httpx.AsyncClient(base_url=KMA_API_BASE_URL) as client:
            response = await client.get("/getUltraSrtNcst", params=params)  #테스트 필요
            logging.info("상태 코드:", response.status_code)
            print("응답 헤더:", response.headers)
            print("응답 내용:", response.text[:500])
            response.raise_for_status()
            
            weather_data = response.json()

            parsed = parse_items(weather_data)

            redis.set(cache_key, str(parsed), ex=CASHE_EXPIRE)
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

# 데이터 파싱
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

#------------------------------------------------------
# 단기예보조회
#------------------------------------------------------
FORECAST_CASHE_EXPIRE = 3 * 60 * 60  # 3시간

# API 요청 파라미터
def get_forecast_params(nx: int, ny: int):
    now = datetime.now()

    target_time = now - timedelta(minutes=10)
    valid_base_times = [2, 5, 8, 11, 14, 17, 20, 23]

    base_hour = -1
    for h in reversed(valid_base_times):
        if target_time.hour >= h:
            base_hour = h
            break
    base_date_target = target_time

    if base_hour == -1:
        base_hour = 23
        base_date_target = target_time - timedelta(days=1)
    base_date = target_time.strftime('%Y%m%d')
    base_time = f"{base_hour:02d}00"

    params = {
        "serviceKey": KMA_SERVICE_KEY,
        "pageNo": 1,
        "numOfRows": 1000,
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }
    return params
    
async def get_current_data(nx: int, ny: int):
    cache_key = f"forecast:{nx}:{ny}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용")
        return eval(cached)
    logging.info("기상청 API에서 날씨 데이터 조회")

    params = get_forecast_params(nx, ny)
    try:
        async with httpx.AsyncClient(base_url=KMA_API_BASE_URL) as client:
            response = await client.get("/getVilageFcst", params=params)  #테스트 필요
            print("상태 코드:", response.status_code)
            print("응답 헤더:", response.headers)
            print("응답 내용:", response.text[:500])
            response.raise_for_status()
            
            weather_data = response.json() # 수정

            parsed = parse_forecast_items(weather_data) # 수정

            redis.set(cache_key, str(parsed), ex=FORECAST_CASHE_EXPIRE)
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")
