import os

from fastapi import FastAPI
from elasticsearch import AsyncElasticsearch

app = FastAPI()
# Whether the environ is dev or prod
prod = True
if prod:
    user = os.environ.get("ELASTIC_USER")
    secret = os.environ.get("ELASTIC_SECRET")
    host = os.environ.get("ELASTIC_HOST")
    es = AsyncElasticsearch([f'https://{user}:{secret}@{localhost}:443'])
else:
    es = AsyncElasticsearch()
    
@app.get('/ping')
async def ping():
    resp = await es.ping()
    return resp

@app.get('/search')
async def index():
    resp = await es.search(
        index="stores-index",
        body={"query":{"match_all": {}}},
        size=20,
    )
    return resp

@app.on_event("shutdown")
async def app_shutdown():
    await es.close()