import os
import httpx
from datetime import datetime, timedelta

KMA_SERVICE_KEY = os.getenv("KMA_SERVICE_KEY")
if KMA_SERVICE_KEY is None:
    raise HTTPException(status_code=500, detail="KEY 못 찾음")