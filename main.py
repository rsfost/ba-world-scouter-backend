from fastapi import FastAPI, Request, Response
from mangum import Mangum
import os
import redis
import time
import json
from datetime import datetime

app = FastAPI()

REDIS_URL=os.environ['REDIS_URL']
REDIS_SOCKET_CONNECT_TIMEOUT = float(os.environ.get("REDIS_SOCKET_CONNECT_TIMEOUT") or '0')
REDIS_SOCKET_TIMEOUT = float(os.environ.get("REDIS_SOCKET_TIMEOUT") or '0')
ENTRY_EXPIRE_MINUTES=float(os.environ.get("ENTRY_EXPIRE_MINUTES") or '10')

db = redis.from_url(REDIS_URL,
    socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT or None,
    socket_timeout=REDIS_SOCKET_TIMEOUT or None
)

@app.get("/worlds")
def list_worlds():
    cursor = 0
    worlds = []
    iterCount = 0
    ITER_LIMIT = 50

    while True:
        cursor, keys = db.scan(cursor=cursor, count=100)
        if keys:
            vals = db.mget(keys)
            worlds.extend([
                {'worldId': int(key), **json.loads(val)}
                for key, val in zip(keys, vals)
                if val is not None
            ])
        iterCount+=1
        if cursor == 0 or iterCount >= ITER_LIMIT:
            break

    return worlds

@app.put("/world/{world}")
async def update_world(world: int, request: Request, response: Response):
    world = int(world)
    data = await request.json()
    y = int(data['y'])
    current_unix_time = int(time.time())

    if world <= 300 or world >= 600 or y < 0 or y > 16383:
        response.status_code = 400
        return {'error': 'invalid world/data'}
    redis_val = {
        'y': y,
        'time': current_unix_time
    }
    db.set(world, json.dumps(redis_val), ex = int(ENTRY_EXPIRE_MINUTES * 60))
    return redis_val

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration_ms = int((time.time() - start_time) * 1000)

    client_ip = request.client.host
    method = request.method
    path = request.url.path
    protocol = request.scope.get("http_version", "1.1")
    status_code = response.status_code
    content_length = response.headers.get("content-length", "-")

    user_agent = request.headers.get("user-agent", "-")
    referer = request.headers.get("referer", "-")

    timestamp = datetime.now().strftime('%d/%b/%Y:%H:%M:%S %z')

    log_entry = (
        f'{client_ip} - - [{timestamp}] "{method} {path} HTTP/{protocol}" '
        f'{status_code} {content_length} "{referer}" "{user_agent}" {duration_ms}ms'
    )
    print(log_entry)

    return response

handler = Mangum(app)
