import os
import json
import logging
import httpx
from fastapi import HTTPException
from datetime import datetime, timedelta
from shared.redis.client import redis
from . import parsers
import asyncio

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

KMA_API_BASE_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0"   #날씨
AIR_API_BASE_URL = "https://apis.data.go.kr/B552584/ArpltnInforInqireSvc"   #대기오염정보
KMA_API_MID_URL = "https://apis.data.go.kr/1360000/MidFcstInfoService"  #중기예보
KMA_SERVICE_KEY = os.getenv("KMA_SERVICE_KEY")

CASHE_EXPIRE = 300  # 5분
MID_TERM_CACHE_EXPIRE = 6 * 60 * 60 # 6시간

#------------------------------------------------------
# 초단기실황조회(현재날씨)
#------------------------------------------------------

# API 요청 파라미터
def get_params(nx: int, ny: int):
    now = datetime.now()

    if now.minute < 40:
        target_time = now - timedelta(hours=1)
    else:
        target_time = now
    base_date = target_time.strftime('%Y%m%d')
    base_time = target_time.strftime('%H00')

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
    live, daily, sky, air = await asyncio.gather(
        get_live_weather(nx, ny),
        get_daily_forecast(nx, ny),
        get_sky_state(nx, ny),
        get_air_state(nx, ny),
        return_exceptions=True
    )
    def safe(data):
        if isinstance(data, Exception):
            # 로그 남기기
            logging.error(f"Weather API failed: {data}")
            return {}
        return data

    live  = safe(live)
    daily = safe(daily)
    sky   = safe(sky)
    air   = safe(air)

    return {**live, **daily, **sky, **air}

#------------------------------------------------------
# 단기예보조회
#------------------------------------------------------
FORECAST_CASHE_EXPIRE = 3 * 60 * 60  # 3시간

# API 요청 파라미터
def get_forecast_params(nx: int, ny: int):
    now = datetime.now()

    target_time = now - timedelta(minutes=45)   #API 제공 시간
    valid_base_times = [2, 5, 8, 11, 14, 17, 20, 23]    # 1일 8회

    base_hour = -1
    base_date = target_time.strftime('%Y%m%d')
    for h in reversed(valid_base_times):
        if target_time.hour >= h:
            base_hour = h
            break

    if base_hour == -1:
        base_hour = 23
        base_date_target = target_time - timedelta(days=1)
        base_date = base_date_target.strftime('%Y%m%d')

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
    
async def get_forecast_data(nx: int, ny: int):
    cache_key = f"forecast:short:{nx}:{ny}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용")
        try:
            return json.loads(cached) 
        except json.JSONDecodeError:
            logging.warning("캐시된 JSON 파싱 오류. API 재호출") # 수정
    logging.info("기상청 API에서 날씨 데이터 조회")

    params = get_forecast_params(nx, ny)
    try:
        transport = httpx.AsyncHTTPTransport(retries=3) # 수정

        async with httpx.AsyncClient(base_url=KMA_API_BASE_URL) as client:
            response = await client.get("/getVilageFcst", params=params)
            logging.info(f"상태 코드: {response.status_code}")
            logging.info(f"응답 헤더: {response.headers}")
            logging.info(f"응답 내용: {response.text[:500]}")
            response.raise_for_status()
            
            forecast_data = response.json()

            parsed = parsers.parse_forecast_items(forecast_data)

            redis.set(cache_key, json.dumps(parsed), ex=FORECAST_CASHE_EXPIRE) # 수정
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        logging.error(f"기상청 API 호출 실패: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        logging.error(f"서버 내부 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

# 초단기실황조회
async def get_live_weather(nx: int, ny: int):
    cache_key = f"weather:{nx}:{ny}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용")
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            logging.warning("캐시된 JSON 파싱 오류. API 재호출")
    logging.info("기상청 API에서 날씨 데이터 조회")

    params = get_params(nx, ny) #api 파라미터 생성
    try:
        async with httpx.AsyncClient(base_url=KMA_API_BASE_URL) as client:
            response = await client.get("/getUltraSrtNcst", params=params)  #테스트 필요
            logging.info(f"상태 코드: {response.status_code}")
            logging.info(f"응답 헤더: {response.headers}")
            logging.info(f"응답 내용: {response.text[:500]}")
            response.raise_for_status()
            
            weather_data = response.json()

            parsed = parsers.parse_items(weather_data)  #데이터 파싱

            redis.set(cache_key, json.dumps(parsed), ex=CASHE_EXPIRE)
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        logging.error(f"기상청 API 호출 실패: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        logging.error(f"서버 내부 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

#단기예보(TMN/TMX)조회 -> 새벽 2시 기준
async def get_daily_forecast(nx: int, ny: int):
    cache_key = f"forecast:{nx}:{ny}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용")
        try:
            return json.loads(cached) 
        except json.JSONDecodeError:
            logging.warning("캐시된 JSON 파싱 오류. API 재호출") # 수정
    logging.info("기상청 API에서 날씨 데이터 조회")

    params = {
        "serviceKey": KMA_SERVICE_KEY,
        "pageNo": 1,
        "numOfRows": 1000,
        "dataType": "JSON",
        "base_date": datetime.now().strftime('%Y%m%d'),
        "base_time": "0200",
        "nx": nx,
        "ny": ny,
    }

    logging.info(f"기상청 API(단기예보 02:00 기준) 데이터 조회 시작")
    try:
        transport = httpx.AsyncHTTPTransport(retries=3) # 수정

        async with httpx.AsyncClient(base_url=KMA_API_BASE_URL) as client:
            response = await client.get("/getVilageFcst", params=params)
            logging.info(f"상태 코드: {response.status_code}")
            logging.info(f"응답 헤더: {response.headers}")
            logging.info(f"응답 내용: {response.text[:500]}")
            response.raise_for_status()
            
            forecast_data = response.json()

            parsed = parsers.parse_tmn_tmx(forecast_data)

            if parsed:
                redis.set(cache_key, json.dumps(parsed), ex=FORECAST_CASHE_EXPIRE) # 수정
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        logging.error(f"기상청 API 호출 실패: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        logging.error(f"서버 내부 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

#초단기예보 - api 파라미터
def get_ultra_params(nx: int, ny: int):
    now = datetime.now()

    if now.minute < 45:
        target_time = now - timedelta(hours=1)
    else:
        target_time = now
    base_date = target_time.strftime('%Y%m%d')
    base_time = target_time.strftime('%H30')

    params = {
        "serviceKey": KMA_SERVICE_KEY,
        "pageNo": 1,
        "numOfRows": 60,
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }
    return params

async def get_sky_state(nx: int, ny: int):
    cache_key = f"weather:sky:{nx}:{ny}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용")
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            logging.warning("캐시된 JSON 파싱 오류. API 재호출")
    logging.info("기상청 API에서 날씨 데이터 조회")

    params = get_ultra_params(nx, ny) #api 파라미터 생성
    try:
        async with httpx.AsyncClient(base_url=KMA_API_BASE_URL) as client:
            response = await client.get("/getUltraSrtFcst", params=params)  #테스트 필요
            logging.info(f"상태 코드: {response.status_code}")
            logging.info(f"응답 헤더: {response.headers}")
            logging.info(f"응답 내용: {response.text[:500]}")
            response.raise_for_status()
            
            weather_data = response.json()

            parsed = parsers.parse_sky_state(weather_data)  #데이터 파싱

            redis.set(cache_key, json.dumps(parsed), ex=CASHE_EXPIRE)
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        logging.error(f"기상청 API 호출 실패: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        logging.error(f"서버 내부 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

#------------------------------------------------------
#대기오염정보조회
#------------------------------------------------------
#위치 JSON 파일 로드
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATH = os.path.join(BASE_DIR, "station.json")
LOCATION_MAP = {}
try:
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        LOCATION_MAP = json.load(f)
    logger.info(f"측정소 매핑 테이블 로드 완료: {len(LOCATION_MAP)}개 지점")
except FileNotFoundError:
    LOCATION_MAP = {}
    logger.warningf("파일을 찾을 수 없습니다: {JSON_PATH}. 기본값(종로구)만 사용됩니다.")

# API 요청 파라미터
def get_air_params(nx: int, ny: int):
    key = f"{nx},{ny}"
    info = LOCATION_MAP.get(key)
    station_name = info["station"] if info else "서대문구"

    params = {
        "serviceKey": KMA_SERVICE_KEY,
        "returnType": "JSON",
        "numOfRows": 1,
        "pageNo": 1,
        "stationName": station_name,
        "dataTerm": "DAILY",
        "ver": "1.3"
    }
    return params

# 대기오염정보조회
async def get_air_state(nx: int, ny: int):
    cache_key = f"weather:air:{nx}:{ny}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용(대기오염정보)")
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            logging.warning("캐시된 JSON 파싱 오류. API 재호출")
    logging.info("기상청 API에서 날씨 데이터 조회 (대기오염정보)")

    params = get_air_params(nx, ny) #api 파라미터 생성
    try:
        async with httpx.AsyncClient(base_url=AIR_API_BASE_URL) as client:
            response = await client.get("/getMsrstnAcctoRltmMesureDnsty", params=params)  #테스트 필요
            logging.info(f"상태 코드: {response.status_code}")
            logging.info(f"응답 헤더: {response.headers}")
            logging.info(f"응답 내용: {response.text[:500]}")
            response.raise_for_status()
            
            air_data = response.json()

            parsed = parsers.parse_air_state(air_data)  #데이터 파싱

            redis.set(cache_key, json.dumps(parsed), ex=CASHE_EXPIRE)
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        logging.error(f"기상청 API 호출 실패: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        logging.error(f"서버 내부 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

#------------------------------------------------------
#2. 시간별 날씨 기능
#------------------------------------------------------
# 초단기예보 조회 (시간별)
async def get_ultra_forecast_data(nx: int, ny: int):
    cache_key = f"forecast:ultra:{nx}:{ny}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용")
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            logging.warning("캐시된 JSON 파싱 오류. API 재호출")
    logging.info("기상청 API에서 날씨 데이터 조회 (초단기예보 - 시간별)")

    params = get_ultra_params(nx, ny) #api 파라미터 생성
    try:
        async with httpx.AsyncClient(base_url=KMA_API_BASE_URL) as client:
            response = await client.get("/getUltraSrtFcst", params=params)  #테스트 필요
            logging.info(f"상태 코드: {response.status_code}")
            logging.info(f"응답 헤더: {response.headers}")
            logging.info(f"응답 내용: {response.text[:500]}")
            response.raise_for_status()
            
            forecast_data = response.json()

            parsed = parsers.parse_ultr_forecast_items(forecast_data)  #데이터 파싱

            redis.set(cache_key, json.dumps(parsed), ex=CASHE_EXPIRE)
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        logging.error(f"기상청 API 호출 실패: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        logging.error(f"서버 내부 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

# 단기 + 초단기 예보 통합 조회 (시간별)
async def get_hourly_forecast_data(nx: int, ny: int):
    results = await asyncio.gather(
        get_ultra_forecast_data(nx, ny),
        get_forecast_data(nx, ny),
        return_exceptions=True
    )
    def safe_get(data):
        if isinstance(data, Exception):
            logging.error(f"API 호출 중 에러 발생: {data}") # 로그에 에러 남김
            return {} # 에러면 빈 딕셔너리 반환
        return data

    ultra = safe_get(results[0])
    short = safe_get(results[1])

    hourly_list = []
    current_time = datetime.now()

    for i in range(24):
        target_time = current_time + timedelta(hours=i)

        t_date = target_time.strftime('%Y%m%d')
        t_time = target_time.strftime('%H00')

        weather_item = {
            "date": t_date,
            "time": t_time,
            "temp": None,       # 기온
            "sky": "정보없음",   # 하늘상태
            "pty": "없음",       # 강수형태
            "rain_amount": "-", # 강수량
            "pop": 0
        }

        if short and t_date in short and t_time in short[t_date]:
            data = short[t_date][t_time]
            weather_item["temp"] = data.get("기온(°C)")
            weather_item["sky"] = data.get("하늘상태", "정보없음")
            weather_item["pty"] = data.get("강수형태", "없음")
            weather_item["rain_amount"] = data.get("1시간 강수량(mm)", "-")
            weather_item["pop"] = data.get("강수확률(%)", 0)

        if ultra and t_date in ultra and t_time in ultra[t_date]:
            data = ultra[t_date][t_time]
            
            # 초단기 예보에 있는 값으로 교체
            if "기온(°C)" in data:
                weather_item["temp"] = data["기온(°C)"]
            if "하늘상태" in data:
                weather_item["sky"] = data["하늘상태"]
            if "강수형태" in data:
                weather_item["pty"] = data["강수형태"]
            if "1시간 강수량(mm)" in data:
                weather_item["rain_amount"] = data["1시간 강수량(mm)"]
            # 참고: 초단기예보에는 강수확률(POP)이 없으므로 단기예보 값을 그대로 유지
            
        # 데이터가 유효한 경우에만 리스트에 추가 (과거 데이터 등 제외 로직이 필요하면 추가)
        if weather_item["temp"] is not None:
             hourly_list.append(weather_item)

    return hourly_list

# ------------------------------------------------------
# 3. 주간 날씨 조회 기능
# ------------------------------------------------------
# 좌표로 구역 코드
def get_mid_reg_code(nx: int, ny: int):
    key = f"{nx},{ny}"
    info = LOCATION_MAP.get(key)
    if info:
        return info["land"], info["ta"]
    else:
        return "11B00000", "11B10101"  #기본값

# 중기예보 API 파라미터 생성
def get_mid_term_params(reg_id: str):
    now = datetime.now()
    
    if now.hour < 6:
        target = now - timedelta(days=1)
        tmFc = target.strftime("%Y%m%d1800")
    elif now.hour < 18:
        tmFc = now.strftime("%Y%m%d0600")
    else:
        tmFc = now.strftime("%Y%m%d1800")

    return {
        "serviceKey": KMA_SERVICE_KEY,
        "pageNo": 1,
        "numOfRows": 10,
        "dataType": "JSON",
        "regId": reg_id,
        "tmFc": tmFc
    }

#중기 기온 조회
async def get_mid_ta(reg_id: str):
    cache_key = f"week:mid:ta:{reg_id}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용")
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            logging.warning("캐시된 JSON 파싱 오류. API 재호출")
    logging.info("기상청 API에서 날씨 데이터 조회 (중기 기온- 주간별)")

    params = get_mid_term_params(reg_id) #api 파라미터 생성
    try:
        async with httpx.AsyncClient(base_url=KMA_API_MID_URL) as client:
            response = await client.get("/getMidTa", params=params)  #테스트 필요
            logging.info(f"상태 코드: {response.status_code}")
            logging.info(f"응답 헤더: {response.headers}")
            logging.info(f"응답 내용: {response.text[:500]}")
            response.raise_for_status()
            
            week_data = response.json()

            parsed = parsers.parse_mid_ta(week_data)  #데이터 파싱

            redis.set(cache_key, json.dumps(parsed), ex=MID_TERM_CACHE_EXPIRE)
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        logging.error(f"기상청 API 호출 실패: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        logging.error(f"서버 내부 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

#중기 육상 기온 조회
async def get_mid_land(reg_id: str):
    cache_key = f"week:mid:land:{reg_id}"
    cached = redis.get(cache_key)
    if cached:
        logging.info("캐시된 날씨 데이터 사용")
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            logging.warning("캐시된 JSON 파싱 오류. API 재호출")
    logging.info("기상청 API에서 날씨 데이터 조회 (중기 육상 - 주간별)")

    params = get_mid_term_params(reg_id) #api 파라미터 생성
    try:
        async with httpx.AsyncClient(base_url=KMA_API_MID_URL) as client:
            response = await client.get("/getMidLandFcst", params=params)  #테스트 필요
            logging.info(f"상태 코드: {response.status_code}")
            logging.info(f"응답 헤더: {response.headers}")
            logging.info(f"응답 내용: {response.text[:500]}")
            response.raise_for_status()
            
            week_data = response.json()

            parsed = parsers.parse_mid_land(week_data)  #데이터 파싱

            redis.set(cache_key, json.dumps(parsed), ex=MID_TERM_CACHE_EXPIRE)
            
            return parsed

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="[401] 기상청 API 인증 실패. 서비스 키를 확인")
        logging.error(f"기상청 API 호출 실패: {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"기상청 API 호출 오류: {e.response.text}")

    except Exception as e:
        logging.error(f"서버 내부 오류: {e}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {e}")

#주간 날씨 조회 통합
async def get_weekly_forecast_data(nx: int, ny: int):
    land_code, ta_code = get_mid_reg_code(nx, ny)

    results = await asyncio.gather(
        get_forecast_data(nx, ny),
        get_mid_ta(ta_code),
        get_mid_land(land_code),
        return_exceptions=True
    )
    def safe(d): return {} if isinstance(d, Exception) else d
    short_data = safe(results[0])
    mid_ta = safe(results[1])
    mid_land = safe(results[2])

    weekly_map = {}

    if short_data:
        short_daily = parsers.aggregate_short_term_to_daily(short_data)
        for date, info in short_daily.items():
            weekly_map[date] = info

    # 4. 중기예보(3~7일) 병합
    today = datetime.now()
    for day in range(3, 8): 
        target_date = (today + timedelta(days=day)).strftime("%Y%m%d")
        
        item = {
            "date": target_date,
            "min_temp": None,
            "max_temp": None,
            "sky_am": "정보없음",
            "sky_pm": "정보없음",
            "pop": 0
        }

        # 기온 데이터 병합
        if mid_ta and target_date in mid_ta:
            item["min_temp"] = mid_ta[target_date]["min_temp"]
            item["max_temp"] = mid_ta[target_date]["max_temp"]
        
        # 날씨 데이터 병합
        if mid_land and target_date in mid_land:
            item["sky_am"] = mid_land[target_date]["sky_am"]
            item["sky_pm"] = mid_land[target_date]["sky_pm"]
            item["pop"] = mid_land[target_date]["pop"]

        # 데이터가 있으면 리스트에 추가
        if item["min_temp"] is not None:
            weekly_map[target_date] = item
            
    # 날짜순 정렬
    weekly_list = list(weekly_map.values())
    weekly_list.sort(key=lambda x: x["date"])

    return weekly_list