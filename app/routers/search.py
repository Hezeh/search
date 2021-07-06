from fastapi import APIRouter, Header
from typing import Optional
import uuid
import requests
from .app_variables import es

router = APIRouter()

@router.get("/search", tags=["search"])
async def search_detail(
    q: str,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
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