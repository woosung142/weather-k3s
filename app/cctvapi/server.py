import requests
import numpy as np
import json
import os

from fastapi import FastAPI, HTTPException, Query
from typing import Optional, Dict, Any, Tuple, List 

# --- FastAPI 앱 설정 ---
app = FastAPI(
    title="ITS CCTV Nearest Search API",
    description="GPS 좌표를 받아 가장 가까운 고속도로 CCTV 정보를 반환하는 API입니다."
)
# ---------------------

def get_nearest_cctv_info(lat: float, lng: float) -> Optional[Tuple[Dict[str, Any], Optional[str]]]:
    """
    ITS 국가교통정보센터 API를 호출하여 입력된 경위도에 가장 가까운 CCTV 정보를 찾습니다.
    """
    # API Key를 함수 내부에 직접 명시했습니다. (원래 방식대로 복구)
    API_KEY = os.getenv("ITS_CCTV_API_KEY")

    if not API_KEY:
        print("오류: ITS_CCTV_API_KEY 환경 변수가 설정 X")
        return None, "서버 설정 오류: API 키가 없습니다."

    # 탐색 범위를 ±0.5도로 설정했습니다.
    minX = str(lng - 0.5)
    maxX = str(lng + 0.5)
    minY = str(lat - 0.5)
    maxY = str(lat + 0.5)

    # cctvType=2: 동영상 파일 요청 / type=ex: 고속도로 CCTV
    api_call = f'https://openapi.its.go.kr:9443/cctvInfo?' \
               f'apiKey={API_KEY}' \
               f'&type=ex&cctvType=2' \
               f'&minX={minX}&maxX={maxX}' \
               f'&minY={minY}&maxY={maxY}' \
               f'&getType=json'

    try:
        w_dataset = requests.get(api_call, timeout=10).json()
        
        # --- DEBUG: Full Response 출력 ---
        print("\n--- DEBUG: Full API Response ---")
        print(json.dumps(w_dataset, indent=2, ensure_ascii=False))
        print("----------------------------------\n")

        cctv_data: List[Dict[str, Any]] = w_dataset.get('response', {}).get('data', [])

        if not cctv_data: 
            return None, "해당 영역에서 CCTV 데이터를 찾을 수 없습니다."
        
        # 최단 거리 계산 (NumPy 활용)
        valid_cctv_data: List[Dict[str, Any]] = []
        valid_coords: List[Tuple[float, float]] = []
        
        for data in cctv_data:
            try:
                coord_y = float(data.get('coordy', ''))
                coord_x = float(data.get('coordx', ''))
                valid_coords.append((coord_y, coord_x))
                valid_cctv_data.append(data)
            except ValueError:
                continue

        if not valid_cctv_data:
            return None, "유효한 좌표를 가진 CCTV 데이터가 없습니다."

        coords = np.array(valid_coords)
        target = np.array((lat, lng))
        distances = np.linalg.norm(coords - target, axis=1)
        min_index = np.argmin(distances)

        return valid_cctv_data[min_index], None
        
    except requests.exceptions.RequestException as e:
        error_msg = f"API 통신 오류 (requests): {e}"
        print(error_msg)
        return None, error_msg
    except Exception as e:
        error_msg = f"데이터 처리 중 예상치 못한 오류: {e}"
        print(error_msg)
        return None, error_msg


@app.get('/get_cctv')
def get_cctv(
    # 위도: 최남단 33.0 ~ 최북단 39.0 (요청에 따라 범위 조정)
    lat: float = Query(..., description="요청할 위치의 위도", ge=33.0, le=39.0), 
    # 경도: 최서단 124.0 ~ 최동단 132.0 (요청에 따라 범위 조정)
    lng: float = Query(..., description="요청할 위치의 경도", ge=124.0, le=132.0) 
):
    """
    GET 요청으로 위도(lat)와 경도(lng)를 받아 가장 가까운 CCTV 정보를 JSON으로 반환합니다.
    """
    print(f"\n--- API 요청 수신: Lat={lat}, Lng={lng} ---")
    
    cctv_info, error_message = get_nearest_cctv_info(lat, lng)

    if error_message:
        raise HTTPException(status_code=500, detail={"status": "error", "message": error_message})
    
    if cctv_info is None:
        raise HTTPException(status_code=404, detail={"status": "fail", "message": "해당 위치 근처에서 CCTV 데이터를 찾을 수 없습니다."})
    
    # 최종 JSON 응답 구성 및 반환
    return {
        "status": "success",
        "cctv_name": cctv_info.get('cctvname', 'Unknown'),
        "cctv_url": cctv_info.get('cctvurl', ''),
        "cctv_type": cctv_info.get('cctvtype', ''),
        "cctv_lat": cctv_info.get('coordy', ''),
        "cctv_lng": cctv_info.get('coordx', '')
    }