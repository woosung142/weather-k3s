from fastapi import APIRouter, HTTPException, Query
from .service import get_current_data, get_hourly_forecast_data

router = APIRouter()

@router.get("/current", summary="현재 날씨 및 상세 날씨 조회", tags=["날씨"])
async def get_current_weather(
    nx: int = Query(60, description="예보지점 X 좌표"),
    ny: int = Query(127, description="예보지점 Y 좌표")
):
    parsed_data = await get_current_data(nx, ny)
    return {
        "위치좌표": {"nx": nx, "ny": ny},
        "날씨": parsed_data
    }
@router.get("/forecast", summary="시간별 날씨 조회", tags=["날씨"])
async def get_forecast_weather(
    nx: int = Query(60, description="예보지점 X 좌표"),
    ny: int = Query(127, description="예보지점 Y 좌표")
):
    parsed_data = await get_hourly_forecast_data(nx, ny)
    return {
        "위치좌표": {"nx": nx, "ny": ny},
        "날씨": parsed_data
    }
@router.get("/week", summary="주간 날씨 조회", tags=["날씨"])
async def get_forecast_weather(
    nx: int = Query(60, description="예보지점 X 좌표"),
    ny: int = Query(127, description="예보지점 Y 좌표")
):
    parsed_data = await get_hourly_forecast_data(nx, ny)
    return {
        "위치좌표": {"nx": nx, "ny": ny},
        "날씨": parsed_data
    }