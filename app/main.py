import os
from aiohttp.helpers import BasicAuth

from fastapi import FastAPI, Header, Request, Response, status
from elasticsearch import AsyncElasticsearch
from typing import Optional
import requests
from pydantic import BaseModel
import json
import base64
import uuid
import aiohttp
# import googleapiclient
from google.oauth2 import service_account
from googleapiclient.discovery import build
import httplib2
import google

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
    # Get all 10 recently clicked categories
    # Get all 10 recently clicked items
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
    lat = None
    lon = None
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f" Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    viewId = json_payload["viewId"]
    if "lat" in json_payload:
        lat = json_payload["lat"] 
    if "lon" in json_payload:
        lon = json_payload["lon"]
    if lat != None and lon != None:
        json_payload["location"] = {
            "lat": lat,
            "lon": lon,
        }
    resp = await item_viewstream_index(viewId, json_payload)
    return resp


async def item_viewstream_index(id, body):
    resp = await es.index(index="item-viewstream", id=id, body=body)
    return resp


@app.post('/clickstream', status_code=200)
async def serp_clickstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f"Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    eventId = json_payload['eventId']
    if "lat" in json_payload:
        lat = json_payload["lat"] 
    if "lon" in json_payload:
        lon = json_payload["lon"]
    if lat != None and lon != None:
        json_payload["location"] = {
            "lat": lat,
            "lon": lon,
        }
    await indexing_func("clickstream", eventId, json_payload)
    return {}

async def indexing_func(index, id, body):
    resp = await es.index(index=index, id=id, body=body)
    return resp


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
                },
                "ProfilePage": {
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
                                "type.keyword": "MerchantProfileItems"
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
    profile_page_impresssions = aggs["ProfilePage"]["doc_count"]
    return {
        "searchImpressions": search_page_impressions,
        "recommendationsImpressions": recs_page_impressions,
        "categoryImpressions": category_page_impressions,
        "totalImpressions": total_impressions,
        "profilePageImpressions": profile_page_impresssions
    }

@app.get('/item/impressions/analytics/{item_id}')
async def item_impressions_analytics(item_id: str):
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
                                "itemId": item_id
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
                                "itemId": item_id
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
                                "itemId": item_id
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
                                "itemId": item_id
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
                },
                "ProfilePage": {
                "filter": {
                    "bool": {
                    "must": [
                        {
                            "match": {
                                "itemId": item_id
                            }
                        },
                        {
                            "match": {
                                "type.keyword": "MerchantProfileItems"
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
    profile_page_impressions = aggs["ProfilePage"]["doc_count"]
    total_impressions = aggs["AllPages"]["doc_count"]
    return {
        "searchImpressions": search_page_impressions,
        "recommendationsImpressions": recs_page_impressions,
        "categoryImpressions": category_page_impressions,
        "totalImpressions": total_impressions,
        "profilePageImpressions": profile_page_impressions
    }

@app.get('/item/clicks/analytics/{item_id}')
async def item_clicks_analytics(item_id: str):
    search_page_clicks = ""
    recs_page_clicks = ""
    category_page_clicks = ""
    total_clicks = ""
    profile_clicks = ""
    resp = await es.search(  
        index='clickstream',
        body={
            "_source": False,
            "aggs": {
                "AllPages": {
                "filter": {
                    "bool": {
                    "must": [
                        {
                            "match": {
                                "itemId": item_id
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
                                "itemId": item_id
                            }
                        },
                        {
                            "match": {
                                "type.keyword": "RecommendationsPageClick"
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
                                "itemId": item_id
                            }
                        },
                        {
                            "match": {
                                "type.keyword": "SearchPageItemClick"
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
                                "itemId": item_id
                            }
                        },
                        {
                            "match": {
                                "type.keyword": "CategoryItemClick"
                            }
                        }
                    ]
                    }
                }
                },
                "ProfilePage": {
                "filter": {
                    "bool": {
                    "must": [
                        {
                            "match": {
                                "itemId": item_id
                            }
                        },
                        {
                            "match": {
                                "type.keyword": "ProfileItemClick"
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
    search_page_clicks = aggs["SearchPage"]["doc_count"]
    recs_page_clicks = aggs["RecommendationsPage"]["doc_count"]
    category_page_clicks = aggs["CategoryPage"]["doc_count"]
    total_clicks = aggs["AllPages"]["doc_count"]
    profile_clicks = aggs["ProfilePage"]["doc_count"]
    return {
        "searchClicks": search_page_clicks,
        "recommendationsClicks": recs_page_clicks,
        "categoryClicks": category_page_clicks,
        "totalClicks": total_clicks,
        "profileClicks": profile_clicks,
    }

@app.get('/clicks/analytics/{merchant_id}')
async def merchant_clicks_analytics(merchant_id: str):
    search_page_clicks = ""
    recs_page_clicks = ""
    category_page_clicks = ""
    total_clicks = ""
    profile_clicks = ""
    resp = await es.search(  
        index='clickstream',
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
                                "type.keyword": "RecommendationsPageClick"
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
                                "type.keyword": "SearchPageItemClick"
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
                                "type.keyword": "CategoryItemClick"
                            }
                        }
                    ]
                    }
                }
                },
                "ProfilePage": {
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
                                "type.keyword": "ProfileItemClick"
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
    search_page_clicks = aggs["SearchPage"]["doc_count"]
    recs_page_clicks = aggs["RecommendationsPage"]["doc_count"]
    category_page_clicks = aggs["CategoryPage"]["doc_count"]
    total_clicks = aggs["AllPages"]["doc_count"]
    profile_clicks = aggs["ProfilePage"]["doc_count"]
    return {
        "searchClicks": search_page_clicks,
        "recommendationsClicks": recs_page_clicks,
        "categoryClicks": category_page_clicks,
        "totalClicks": total_clicks,
        "profileClicks": profile_clicks,
    }

class PurchaseModel(BaseModel):
    packageName: Optional[str]
    token: Optional[str]
    subscriptionId: Optional[str]

@app.post('/purchases/subscriptions')
async def verify_purchase(purchase: PurchaseModel):
    # credentials = service_account.Credentials.from_service_account_file("./service_account.json")
    # scoped_credentials = credentials.with_scopes(
    # ['https://www.googleapis.com/auth/androidpublisher'])
    credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/androidpublisher'])
    service = build("androidpublisher", "v3", credentials=credentials)
    # credentials = ServiceAccountCredentials.from_json_keyfile_name(
    #   'service-account-abcdef123456.json',
    # scopes='https://www.googleapis.com/auth/tasks')

    # # Create an httplib2.Http object to handle our HTTP requests and authorize
    # # it with the Credentials.
    # http = httplib2.Http()
    # http = credentials.authorize(http)

    # service = build("tasks", "v1", http=http)
    #Use the token your API got from the app to verify the purchase
    result = service.purchases().subscriptions().get(packageName=purchase.packageName, subscriptionId=purchase.subscriptionId, token=purchase.token).execute()
    # key = 'AIzaSyDx3sQEe0FwOcrUxthdrYeTO-CuZfa1nrc'
    # google_url = f'https://androidpublisher.googleapis.com/androidpublisher/v3/applications/{purchase.packageName}/purchases/subscriptions/{purchase.subscriptionId}/tokens/{purchase.token}?key={key}'
    # async with aiohttp.ClientSession() as session:
    #     async with session.get(google_url, headers={
    #         'Authorization': 'Bearer 207309330467-jcfa7l875gfh46s0thqo5atpkagartoi.apps.googleusercontent.com',
    #         'Accept': 'application/json'
    #     }) as response:

    #         print("Status:", response.status)
    #         print("Content-type:", response.headers['content-type'])

    #         resp_json = await response.json()
    #         print(resp_json)
    print(result)
    return {}
    


@app.on_event("shutdown")
async def app_shutdown():
    await es.close()
