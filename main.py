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
    worlds = {}
    while True:
        cursor, keys = db.scan(cursor=cursor, count=100)
        if keys:
            values = [json.loads(v) if v is not None else None for v in db.mget(keys)]
            worlds.update(dict(zip(keys, values)))
        if cursor == 0:
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
