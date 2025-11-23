import os
from redis import Redis

redis = Redis(
    host="localhost", # 수정
    port=6379,
    decode_responses=True
)
