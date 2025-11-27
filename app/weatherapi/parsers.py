import logging
from datetime import datetime, timedelta
from fastapi import HTTPException

# ----------------------------------------------
# api/weather/current
# ----------------------------------------------

# 데이터 파싱 (초단기실황) -> /current 에서 사용
def parse_items(data:dict):
    try:
        items = data["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
        logging.warning(f"초단기실황 파싱 실패: {data}")
        return {}

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

# 단기예보(TMN/TMX) 파싱 -> /current 에서 사용
def parse_tmn_tmx(data: dict):
    try:
        items = data["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
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

#하늘상태 파싱 -> /current 에서 사용
def parse_sky_state(data: dict):
    try:
        items = data["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
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

#대기상태 파싱 -> /current 에서 사용
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

# ----------------------------------------------
# api/weather/forecast
# ----------------------------------------------

# 데이터 파싱 (단기예보) -> /forecast 에서 사용
def parse_forecast_items(data:dict):
    try:
        items = data["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
        logging.warning(f"단기예보 데이터 파싱 실패 (기상청 응답 오류 또는 데이터 없음): {data}")
        return {}

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

#초단기예보 데이터 파싱 -> /forecast 에서 사용
def parse_ultr_forecast_items(data:dict):
    try:
        items = data["response"]["body"]["items"]["item"]
    except (KeyError, TypeError):
        return {}
    
    category = {
        "T1H": "기온(°C)",
        "RN1": "1시간 강수량(mm)",
        "SKY": "하늘상태",  #코드값
        "PTY": "강수형태"   #코드값
    }

    sky_map = {
        "1": "맑음",
        "3": "구름많음",
        "4": "흐림"
    }
    rain_map = {
        "0": "없음",
        "1": "비",
        "2": "비/눈",
        "3": "눈",
        "5": "빗방울",
        "6": "빗방울눈날림",
        "7": "눈날림"
    }

    parsed = {}
    for item in items:
        fcstDate = item["fcstDate"]
        fcstTime = item["fcstTime"]
        cat = item["category"]
        value = item["fcstValue"]

        if cat in category:
            label = category[cat]

            if cat == "SKY":
                value = sky_map.get(value, "구름많음")
            elif cat == "PTY":
                value = rain_map.get(value, "없음")
            else:
                value = float(value) if value.replace('.', '', 1).isdigit() else value

            if fcstDate not in parsed:
                parsed[fcstDate] = {}
            if fcstTime not in parsed[fcstDate]:
                parsed[fcstDate][fcstTime] = {}

            parsed[fcstDate][fcstTime][label] = value
    return parsed

# ----------------------------------------------
# api/weather/week
# ----------------------------------------------
def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return value

#중기 기온 파싱 (3일~10일 후) -> /week 에서 사용
def parse_mid_ta(data:dict):
    try:
        item = data["response"]["body"]["items"]["item"][0]
    except (KeyError, IndexError, TypeError):
        return {}

    parsed = {}
    today = datetime.now()
    
    for day in range(3, 8): 
        target_date = (today + timedelta(days=day)).strftime("%Y%m%d")
        
        min_temp = item.get(f"taMin{day}")
        max_temp = item.get(f"taMax{day}")

        if min_temp is not None and max_temp is not None:
            parsed[target_date] = {
                "min_temp": safe_float(min_temp),
                "max_temp": safe_float(max_temp)
            }
    return parsed

#중기 육상 파싱 (3일~10일 후) -> /week 에서 사용
def parse_mid_land(data:dict):
    try:
        item = data["response"]["body"]["items"]["item"][0]
    except (KeyError, IndexError, TypeError):
        return {}

    parsed = {}
    today = datetime.now()

    for day in range(3, 8):
        target_date = (today + timedelta(days=day)).strftime("%Y%m%d")
        
        # 하늘상태
        sky_am = item.get(f"wf{day}Am")
        sky_pm = item.get(f"wf{day}Pm")
        
        if not sky_am: sky_am = item.get(f"wf{day}")
        if not sky_pm: sky_pm = item.get(f"wf{day}")

        # 강수확률
        pop_am = item.get(f"rnSt{day}Am", 0)
        pop_pm = item.get(f"rnSt{day}Pm", 0)
        max_pop = max(int(pop_am or 0), int(pop_pm or 0))

        parsed[target_date] = {
            "sky_am": sky_am,
            "sky_pm": sky_pm,
            "pop": max_pop
        }
    return parsed

#단기예보 데이터를 일별 요약으로 변환 -> /week 에서 사용
def aggregate_short_term_to_daily(short_data: dict):
    daily_summary = {}
    
    for date, times in short_data.items():
        temps = []
        skies = []
        rain_probs = []

        for time, val in times.items():
            if "기온(°C)" in val: temps.append(safe_float(val["기온(°C)"]))
            if "하늘상태" in val: skies.append(val["하늘상태"])
            if "강수확률(%)" in val: rain_probs.append(int(safe_float(val["강수확률(%)"])))

        if not temps: continue

        import collections
        most_common_sky = collections.Counter(skies).most_common(1)[0][0] if skies else "맑음"
        max_pop = max(rain_probs) if rain_probs else 0

        daily_summary[date] = {
            "date": date,
            "min_temp": min(temps),
            "max_temp": max(temps),
            "sky_am": most_common_sky,
            "sky_pm": most_common_sky,
            "pop": max_pop
        }
    
    return daily_summary