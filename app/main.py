from fastapi import FastAPI
from elasticsearch import AsyncElasticsearch

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from routers import search
from routers import recommendations
from routers import indexing
from routers import item_clicks
from routers import item_impressions
from routers import merchant_clicks
from routers import merchant_impressions
from routers import past_searches
from routers import category
from routers import clickstream
from routers import flutterwave_pay
from routers import flutterwave_webhook
from routers import merchant
from routers import play_purchases
from routers import item_viewstream
from routers import sendy


app = FastAPI()
es = AsyncElasticsearch()

app.include_router(search.router)
app.include_router(recommendations.router)
app.include_router(indexing.router)
app.include_router(item_clicks.router)
app.include_router(item_impressions.router)
app.include_router(merchant_clicks.router)
app.include_router(merchant_impressions.router)
app.include_router(past_searches.router)
app.include_router(category.router)
app.include_router(clickstream.router)
app.include_router(flutterwave_webhook.router)
app.include_router(flutterwave_pay.router)
app.include_router(merchant.router)
app.include_router(play_purchases.router)
app.include_router(item_viewstream.router)
app.include_router(sendy.router)


@app.get("/")
async def home_page():
    return {"Message": "Welcome"}

@app.get("/ping")
async def ping():
    resp = await es.ping()
    return resp

@app.on_event("shutdown")
async def app_shutdown():
    await es.close()
