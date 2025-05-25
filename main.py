from fastapi import FastAPI, Request, Response
from mangum import Mangum
import os
import redis
import time
import json

app = FastAPI()

db = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    username=os.environ['REDIS_USER'],
    password=os.environ['REDIS_PASSWORD'],
    ssl=os.environ.get('REDIS_SSL'),
    decode_responses=True,
    db=0
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
    db.set(world, json.dumps(redis_val), ex = 1 * 3600)
    return redis_val

handler = Mangum(app)
