import os
from redis import Redis

redis = Redis(
    host="redis-service", # 수정
    port=6379,
    decode_responses=True
)
