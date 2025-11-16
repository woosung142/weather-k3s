import os
from redis import Redis

redis = Redis(
    host="redis-service",
    port=6379,
    decode_responses=True
)
