from fastapi import APIRouter, Header
from typing import Optional
import uuid
import requests
from .app_variables import es
from .recs_categories import recs_categories

router = APIRouter()

@router.get("/recs", tags=["recommendations"])
async def recs(lat: Optional[float] = None, lon: Optional[float] = None, x_forwarded_for: Optional[str] = Header(None),):
    # Get the userId or deviceId
    # Get browsing history
    # Sort the items based on past
    # clicked items
    latitude = lat
    longitude = lon
    recsId = uuid.uuid4() 
    if latitude == None and longitude == None:
        key = "a390f3d3aa104eb0b008f3ff8982c055"
        r = requests.get(
            f"https://api.ipgeolocation.io/ipgeo?apiKey={key}&ip={x_forwarded_for}"
        )
        if r.status_code == 200:
            ip_response = r.json()
            latitude = ip_response["latitude"]
            longitude = ip_response["longitude"]
        else:
            latitude = 0
            longitude = 0
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

@router.get("/test-recs", tags=["recommendations"])
async def test_recs(lat: Optional[float] = None, lon: Optional[float] = None, x_forwarded_for: Optional[str] = Header(None),):
    latitude = lat
    longitude = lon
    recsId = uuid.uuid4() 
    if latitude == None and longitude == None:
        key = "a390f3d3aa104eb0b008f3ff8982c055"
        r = requests.get(
            f"https://api.ipgeolocation.io/ipgeo?apiKey={key}&ip={x_forwarded_for}"
        )
        if r.status_code == 200:
            ip_response = r.json()
            latitude = ip_response["latitude"]
            longitude = ip_response["longitude"]
        else:
            latitude = 0
            longitude = 0
    resp = await es.search(
        index="items",
        body={
            "query": {
                "function_score": {
                "score_mode": "sum",
                "functions": [
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
                                "origin": {
                                    "lat": 0.5,
                                    "lon": 5
                                },
                                "offset": "0km",
                                "scale": "4km"
                            }
                        }
                    }
                ]
                },
                "sort": [
                    {
                        "_geo_distance": {
                            "location": {
                                "lat": 0.6,
                                "lon": 5
                            },
                            "order": "asc",
                            "unit": "km",
                            "ignore_unmapped": True
                        }
                    }
                ]
            }
        }
    )
    docs = resp["hits"]["hits"]
    recs_list = []
    # Get all the different categories
    categories = []
    if len(docs) != 0:
        for doc in docs:
            # Extract the catory of the document
            doc_category = doc["_source"]["category"]
            if (doc_category not in categories):
                # Add the category to the categories list
                categories.append(doc_category)
    for category in categories:
        recs = []
        if len(docs) != 0:
            for doc in docs:
                doc_category = doc["_source"]["category"]
                if doc_category == category:
                    recs.append(doc["_source"])
        recs_list.append(
            {
                "category": f"{category}"
            },
            {
                "items": recs
            }
        )
    # buckets = resp["aggregations"]["categories"]["buckets"]
    # for bucket in buckets:
    #     category = bucket["key"]
    #     recs = []
    #     if len(docs) != 0:
    #         for doc in docs:
    #             doc_category = doc["_source"]["category"]
    #             if doc_category == category:
    #                 recs.append(doc["_source"])
    #     recs_list.append({"category": f"{category}", "items": recs})
    return {
        "recsId": recsId,
        "recommendations": recs_list
    }