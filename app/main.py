import os

from fastapi import FastAPI, Header, Request, Response, status
from elasticsearch import AsyncElasticsearch
from typing import Optional
import requests
from pydantic import BaseModel
import json
import base64
import uuid

app = FastAPI()
es = AsyncElasticsearch()

@app.get("/")
async def home_page():
    return {"Message": "Welcome"}

@app.get("/ping")
async def ping():
    resp = await es.ping()
    return resp

@app.get("/search")
async def search_detail(
    q: str,
    lat: float,
    lon: float,
    user_agent: Optional[str] = Header(None),
    x_forwarded_for: Optional[str] = Header(None),
):
    parsed_results = []
    latitude = lat
    longitude = lon
    searchId = uuid.uuid4()  
    if latitude == None and longitude == None:
        key = "a390f3d3aa104eb0b008f3ff8982c055"
        r = requests.get(
            f"https://api.ipgeolocation.io/ipgeo?apiKey={key}&ip={x_forwarded_for}"
        )
        ip_response = r.json()
        latitude = ip_response["latitude"]
        longitude = ip_response["longitude"]
    results = await es.search(
            index="items",
            body={
                "track_scores": True,
                "query": {
                    "function_score": {
                        "score_mode": "sum",
                        "query": {
                            "multi_match": {
                                "query": q,
                                "fields": [
                                    "title^20",
                                    "description^15",
                                    "category^5",
                                    "subCategory^18",
                                    "businessName^5",
                                    "locationDescription^10",
                                    "businessDescription^5"
                                ],
                                "tie_breaker": 0.3,
                                "fuzziness": "AUTO"
                            }
                        },
                        "functions": [
                            {
                                "weight": 4,
                                "filter": {
                                    "bool": {
                                        "should": [
                                            {
                                                "term": {
                                                    "promoted": True
                                                }
                                            }
                                        ]
                                    }
                                }
                                },
                                {
                                "weight": 1.2,
                                "filter": {
                                    "bool": {
                                        "should": [
                                            {
                                                "term": {
                                                    "inStock": True
                                                }
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "weight": 2.1,
                                "gauss": {
                                    "location": {
                                        "origin": {"lat": latitude, "lon": longitude},
                                        "offset": "0km",
                                        "scale": "4km"
                                    }
                                }
                            }
                        ]
                    }
                },
                "sort": [
                    {
                        "_geo_distance": {
                            "location": {"lat": latitude, "lon": longitude},
                            "order": "asc",
                            "unit": "km",
                            "ignore_unmapped": True
                        }
                    }
                ],
                "aggs": {"map_zoom": {"geo_bounds": {"field": "location"}}}
            },
            size=20
        )
    hits = results["hits"]["hits"]
    if len(hits) != 0:
        for result in hits:
            source = result["_source"]
            sort = result["sort"][0]
            source["distance"] = sort
            parsed_results.append(source)
        bounds = results["aggregations"]["map_zoom"]["bounds"]
        return {
            "items": parsed_results,
            "bounds": bounds,
            "searchId": searchId
        }
    else:
        return {}

async def item_index(id, body):
    resp = await es.index(index="items", id=id, body=body)
    return resp


@app.post("/index")
async def index_es(request: Request):
    envelope = await request.body()
    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    id = json_payload["itemId"]
    latitude = json_payload["location"]["_latitude"]
    longitude = json_payload["location"]["_longitude"]
    json_payload["location"] = {"lat": latitude, "lon": longitude}
    resp = await item_index(id, json_payload)
    return resp


async def item_update(id, body):
    resp = await es.update(index="items", id=id, body={"doc": body})
    return resp


@app.post("/update")
async def update_index(request: Request):
    envelope = await request.body()
    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    id = json_payload["itemId"]
    latitude = json_payload["location"]["_latitude"]
    longitude = json_payload["location"]["_longitude"]
    json_payload["location"] = {"lat": latitude, "lon": longitude}
    resp = await item_update(id, json_payload)
    return resp

async def item_delete(id):
    # TODO: If doc exists then delete else just return
    resp = await es.delete(index="items", id=id)
    return resp


@app.post("/delete")
async def delete_document(request: Request):
    envelope = await request.body()
    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    id = json_payload["itemId"]
    resp = await item_delete(id)
    return resp


@app.get("/recs")
async def recs(lat: Optional[float] = None, lon: Optional[float] = None, x_forwarded_for: Optional[str] = Header(None),):
    latitude = lat
    longitude = lon
    recsId = uuid.uuid4() 
    if latitude == None and longitude == None:
        key = "a390f3d3aa104eb0b008f3ff8982c055"
        r = requests.get(
            f"https://api.ipgeolocation.io/ipgeo?apiKey={key}&ip={x_forwarded_for}"
        )
        ip_response = r.json()
        latitude = ip_response["latitude"]
        longitude = ip_response["longitude"]
    resp = await es.search(
        index="items",
        body={
            "query": {
                "bool": {
                    "should": [
                        {"terms": {"category.keyword": ["Food"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Home and Kitchen"], "boost": 10.0}},
                        {"terms": {"category.keyword": ["Arts and Craft"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Automotive"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Baby"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Beauty and Personal Care"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Computers"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Electronics"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Household Essentials"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Industrial and Scientific"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Luggage"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Men's Fashion"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Patio and Garden"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Pet Supplies"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Smart Home"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Sports, Fitness and Outdoors"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Tools and Home Improvement"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Toys and Games"], "boost": 1.0}},
                        {"terms": {"category.keyword": ["Women's Fashion"], "boost": 1.0}},
                        {
                            "distance_feature": {
                                "field": "location",
                                "pivot": "1000m",
                                "origin": {
                                    "lat": latitude,
                                    "lon": longitude
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "categories": {
                    "terms": {"field": "category.keyword", "size": 20},
                    "aggs": {
                        "sub_categories": {
                            "terms": {
                                "field": "subCategory.keyword",
                                "size": 10,
                                "min_doc_count": 1
                            }
                        }
                    },
                }
            },
        },
    )
    docs = resp["hits"]["hits"]
    recs_list = []
    buckets = resp["aggregations"]["categories"]["buckets"]
    for bucket in buckets:
        category = bucket["key"]
        recs = []
        if len(docs) != 0:
            for doc in docs:
                doc_category = doc["_source"]["category"]
                if doc_category == category:
                    recs.append(doc["_source"])
        recs_list.append({"category": f"{category}", "items": recs})
    return {
        "recsId": recsId,
        "recommendations": recs_list
    }


@app.post("/item-viewstream", status_code=200)
async def item_viewstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f" Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    viewId = json_payload["viewId"]
    lat = json_payload["lat"]
    lon = json_payload["lon"]
    json_payload["location"] = {
        "lat": lat,
        "lon": lon,
    }
    resp = await item_viewstream_index(viewId, json_payload)
    return resp


async def item_viewstream_index(id, body):
    resp = await es.index(index="item-viewstream", id=id, body=body)
    return resp


@app.post('/serp-clickstream', status_code=200)
async def serp_clickstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f"Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    # Index every searchId 
    # Get the query
    # Add a click to all clicks index
    # Add a click to the item-clickstream index
    # Add a click to the merchant-items-clickstream index
    # TODO Add a click to the user-clickstream-category index
    searchId = json_payload["searchId"]
    eventId = json_payload['eventId']
    itemId = json_payload["itemId"]
    merchantId = json_payload["merchantId"]
    lat = json_payload["lat"]
    lon = json_payload["lon"]
    json_payload["location"] = {
        "lat": lat,
        "lon": lon,
    }
    await indexing_func("all-items-clicks", eventId, json_payload)
    await indexing_func("search-clicks", searchId, json_payload)
    await indexing_func("item-clickstream", itemId, json_payload)
    await indexing_func("merchant-items-clickstream", merchantId, json_payload)
    return {"Message": "Done Indexing"}

async def indexing_func(index, id, body):
    resp = await es.index(index=index, id=id, body=body)
    return resp

@app.post('/profile-clickstream', status_code=200)
async def profile_clickstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f"Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    merchantId = json_payload["merchantId"]
    lat = json_payload["lat"]
    lon = json_payload["lon"]
    json_payload["location"] = {
        "lat": lat,
        "lon": lon,
    }
    await indexing_func("merchant-profile-clickstream", merchantId, json_payload)
    return {"Message": "Done Indexing"}

@app.post('/recs-clickstream', status_code=200)
async def recs_clickstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f"Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    merchantId = json_payload["merchantId"]
    recsId = json_payload["recsId"]
    lat = json_payload["lat"]
    lon = json_payload["lon"]
    json_payload["location"] = {
        "lat": lat,
        "lon": lon,
    }
    eventId = json_payload['eventId']
    itemId = json_payload["itemId"]
    await indexing_func("all-items-clicks", eventId, json_payload)
    await indexing_func("item-clickstream", itemId, json_payload)
    await indexing_func("recs-clicks", recsId, json_payload)
    await indexing_func("merchant-items-clickstream", merchantId, json_payload)
    return {"Message": "Done Indexing"}

@app.post('/category-item-clickstream', status_code=200)
async def category_item_clickstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f"Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    merchantId = json_payload["merchantId"]
    eventId = json_payload["eventId"]
    itemId = json_payload["itemId"]
    lat = json_payload["lat"]
    lon = json_payload["lon"]
    json_payload["location"] = {
        "lat": lat,
        "lon": lon,
    }
    await indexing_func("all-items-clicks", eventId, json_payload)
    await indexing_func("item-clickstream", itemId, json_payload)
    await indexing_func("merchant-items-clickstream", merchantId, json_payload)
    return {"Message": "Done Indexing"}

@app.post('/category-all-clickstream', status_code=200)
async def category_all_clickstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f"Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    eventId = json_payload["event"]
    lat = json_payload["lat"]
    lon = json_payload["lon"]
    json_payload["location"] = {
        "lat": lat,
        "lon": lon,
    }
    await indexing_func("category-all-clickstream", eventId, json_payload)
    return {"Message": "Done Indexing"}

@app.post('/profile-item-clickstream', status_code=200)
async def profile_item_clickstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f"Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    searchId = json_payload["searchId"]
    eventId = json_payload['eventId']
    itemId = json_payload["itemId"]
    merchantId = json_payload["merchantId"]
    lat = json_payload["lat"]
    lon = json_payload["lon"]
    json_payload["location"] = {
        "lat": lat,
        "lon": lon,
    }
    await indexing_func("all-items-clicks", eventId, json_payload)
    await indexing_func("profile-item-clicks", searchId, json_payload)
    await indexing_func("item-clickstream", itemId, json_payload)
    await indexing_func("merchant-items-clickstream", merchantId, json_payload)
    return {"Message": "Done Indexing"}

@app.get('/category/{category_name}')
async def category_items(category_name: str, lat: Optional[float] = None, lon: Optional[float] = None):
    parsed_results = []
    resp = await es.search(  
        index='items',
        body={
            "query": {
                "match": {
                    "category.keyword": category_name
                }
            }
        }
    )
    hits = resp["hits"]["hits"]
    if len(hits) != 0:
        for result in hits:
            source = result["_source"]
            parsed_results.append(source)
    return {
        'items': parsed_results
    }


@app.get('/merchant/{merchant_id}')
async def merchant_items(merchant_id: str):
    parsed_results = []
    resp = await es.search(  
        index='items',
        body={
            "query": {
                "match": {
                    "userId.keyword": merchant_id
                }
            }
        }
    )
    hits = resp["hits"]["hits"]
    if len(hits) != 0:
        for result in hits:
            source = result["_source"]
            parsed_results.append(source)
    return {
        'items': parsed_results
    }

@app.get('/impressions/analytics/{merchant_id}')
async def merchant_impressions_analytics(merchant_id: str):
    search_page_impressions = ""
    recs_page_impressions = ""
    category_page_impressions = ""
    total_impressions = ""
    resp = await es.search(  
        index='item-viewstream',
        body={
            "_source": False,
            "aggs": {
                "AllPages": {
                "filter": {
                    "bool": {
                    "must": [
                        {
                            "match": {
                                "merchantId": merchant_id
                            }
                        }
                    ]
                    }
                }
                },
                "RecommendationsPage": {
                "filter": {
                    "bool": {
                    "must": [
                        {
                            "match": {
                                "merchantId": merchant_id
                            }
                        },
                        {
                            "match": {
                                "type.keyword": "Recommendations"
                            }
                        }
                    ]
                    }
                }
                },
                "SearchPage": {
                "filter": {
                    "bool": {
                    "must": [
                        {
                            "match": {
                                "merchantId": merchant_id
                            }
                        },
                        {
                            "match": {
                                "type.keyword": "Search"
                            }
                        }
                    ]
                    }
                }
                },
                "CategoryPage": {
                "filter": {
                    "bool": {
                    "must": [
                        {
                            "match": {
                                "merchantId": merchant_id
                            }
                        },
                        {
                            "match": {
                                "type.keyword": "CategoryViewAll"
                            }
                        }
                    ]
                    }
                }
                }
            }
            }
    )
    aggs = resp["aggregations"]
    search_page_impressions = aggs["SearchPage"]["doc_count"]
    recs_page_impressions = aggs["RecommendationsPage"]["doc_count"]
    category_page_impressions = aggs["CategoryPage"]["doc_count"]
    total_impressions = aggs["AllPages"]["doc_count"]
    return {
        "searchImpressions": search_page_impressions,
        "recommendationsImpressions": recs_page_impressions,
        "categoryImpressions": category_page_impressions,
        "totalImpressions": total_impressions
    }

@app.on_event("shutdown")
async def app_shutdown():
    await es.close()
