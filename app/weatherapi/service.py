import os
import json
import logging
import httpx
from fastapi import HTTPException
from datetime import datetime, timedelta
from shared.redis.client import redis
import asyncio

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

KMA_API_BASE_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0"   #날씨
AIR_API_BASE_URL = "https://apis.data.go.kr/B552584/ArpltnInforInqireSvc"   #대기오염정보
KMA_SERVICE_KEY = os.getenv("KMA_SERVICE_KEY")

#------------------------------------------------------
# 초단기실황조회(현재날씨)
#------------------------------------------------------
CASHE_EXPIRE = 300  # 5분

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
    return {**live, **daily, **sky, **air}

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
        "PTY": "강수형태", #코드값
        "WSD": "풍속(m/s)",
        "VEC": "풍향(deg)",
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

    target_time = now - timedelta(minutes=10)   #API 제공 시간
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
    cache_key = f"forecast:{nx}:{ny}"
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

            parsed = parse_forecast_items(forecast_data)

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

# 데이터 파싱
def parse_forecast_items(data:dict):
    try:
        items = data["response"]["body"]["items"]["item"]
    except KeyError:
        raise HTTPException(status_code=500, detail="기상청 응답 구조가 올바르지 않습니다.")

    category_map = {
        "POP": "강수확률(%)",
        "PTY": "강수형태",  #코드값
        "PCP": "1시간 강수량(mm)",  #코드값
        "REH": "습도(%)",
        "SNO": "1시간 신적설(cm)",  #코드값
        "SKY": "하늘상태",  #코드값
        "TMP": "기온(°C)",
        "TMN": "일 최저기온(°C)",
        "TMX": "일 최고기온(°C)",
        "UUU": "풍속(동서성분)(m/s)",
        "VVV": "풍속(남북성분)(m/s)",
        "WAV": "파고(m)",
        "VEC": "풍향(deg)",
        "WSD": "풍속(m/s)"  #코드값
    }

    rain_type_map = {
        "0": "없음",
        "1": "비",
        "2": "비/눈",
        "3": "눈",
        "4": "소나기"
    }
    sky_type_map = {
        "1": "맑음",
        "3": "구름많음",
        "4": "흐림"
    }
    pcp_type_map = {    # 강수량(PCP) 코드 변환
        "1": "약한 비", # 시간당 3mm 미만
        "2": "보통 비", # 시간당 3mm 이상 15mm 미만
        "3": "강한 비"  # 시간당 15mm 이상
    }

    sno_type_map = {    # 눈의 양(SNO) 코드 변환
        "1": "보통 눈", # 시간당 1cm 미만
        "2": "많은 눈"  # 시간당 1cm 이상
    }
    
    wsd_type_map = {    # 풍 속(WSD) 코드 변환
        "1": "약한 바람",       # 4m/s 이상
        "2": "약간 강한 바람",  # 4m/s 이상 9m/s 미만
        "3": "강한 바람"        # 9m/s 이상
    }

    parsed = {}
    for item in items:
        category = item["category"]
        value = item["fcstValue"]
        fcstDate = item["fcstDate"]
        fcstTime = item["fcstTime"]

        if category in category_map:
            label = category_map[category]

            if category == "PTY":  # 강수형태 코드 변환
                value = rain_type_map.get(value, "알 수 없음")
            elif category == "SKY":  # 하늘상태 코드 변환
                value = sky_type_map.get(value, "알 수 없음")
            elif category == "PCP":  # 강수량 코드 변환
                value = pcp_type_map.get(value, value)
            elif category == "SNO":  # 눈의 양 코드 변환
                value = sno_type_map.get(value, value)
            elif category == "WSD":  # 풍 속 코드 변환
                value = wsd_type_map.get(value, value)
            else:
                value = float(value) if value.replace('.', '', 1).isdigit() else value

            if fcstDate not in parsed:
                parsed[fcstDate] = {}
            if fcstTime not in parsed[fcstDate]:
                parsed[fcstDate][fcstTime] = {}

            parsed[fcstDate][fcstTime][label] = value
    return parsed

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

            parsed = parse_items(weather_data)  #데이터 파싱

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

# 단기예보(TMN/TMX) 파싱
def parse_tmn_tmx(data: dict):
    try:
        items = data["response"]["body"]["items"]["item"]
    except KeyError:
        return {}

    parsed = {}
    today_str = datetime.now().strftime('%Y%m%d')

    for item in items:
        if item["fcstDate"] != today_str:
            continue

        category = item["category"]
        
        if category == "TMN":
            parsed["일 최저기온(°C)"] = float(item["fcstValue"])
        elif category == "TMX":
            parsed["일 최고기온(°C)"] = float(item["fcstValue"])
    return parsed

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

            parsed = parse_tmn_tmx(forecast_data)

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

#하늘상태 파싱
def parse_sky_state(data: dict):
    try:
        items = data["response"]["body"]["items"]["item"]
    except KeyError:
        return {}

    sky_map = {
        "1": "맑음",
        "3": "구름많음",
        "4": "흐림"
    }

    for item in items:
        if item["category"] == "SKY":
            code = item["fcstValue"]
            return {"하늘상태": sky_map.get(code, "구름많음")}
            
    return {"하늘상태": "구름많음"}

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

            parsed = parse_sky_state(weather_data)  #데이터 파싱

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
try:
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        NX_NY_TO_STATION = json.load(f)
    logger.info(f"측정소 매핑 테이블 로드 완료: {len(NX_NY_TO_STATION)}개 지점")
except FileNotFoundError:
    NX_NY_TO_STATION = {}
    logger.warningf("파일을 찾을 수 없습니다: {JSON_PATH}. 기본값(종로구)만 사용됩니다.")

# API 요청 파라미터
def get_air_params(nx: int, ny: int):
    key = f"{nx},{ny}"
    station_name = NX_NY_TO_STATION.get(key, "종로구")  #기본값 종로구

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

            parsed = parse_air_state(air_data)  #데이터 파싱

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

def parse_air_state(data: dict):
    try:
        items = data["response"]["body"]["items"]
        if not items:
            return {"미세먼지": "정보없음", "초미세먼지": "정보없음"}
            
        item = items[0]

        grade_map = {
            "1": "좋음",
            "2": "보통",
            "3": "나쁨",
            "4": "매우나쁨"
        }
        
        pm10_code = item.get("pm10Grade1h") or item.get("pm10Grade")
        pm25_code = item.get("pm25Grade1h") or item.get("pm25Grade")

        pm10_status = grade_map.get(str(pm10_code), "정보없음")
        pm25_status = grade_map.get(str(pm25_code), "정보없음")

        pm10_value = item.get("pm10Value", "-")
        pm25_value = item.get("pm25Value", "-")
        data_time = item.get("dataTime", "")

        return {
            "미세먼지": pm10_status,          # 예: "보통"
            "초미세먼지": pm25_status,        # 예: "나쁨"
            "미세먼지농도": pm10_value,       # 예: "73"
            "초미세먼지농도": pm25_value,     # 예: "44"
            "측정시간": data_time             # 예: "2020-11-25 13:00"
        }

    except (KeyError, IndexError, AttributeError):
        return {"미세먼지": "정보없음", "초미세먼지": "정보없음"}