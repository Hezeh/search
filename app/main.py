import os

from fastapi import FastAPI, Header, Request, Response, status
from elasticsearch import AsyncElasticsearch
from typing import List, Optional
import requests
from pydantic import BaseModel
import json
from geojson import Point
import base64

app = FastAPI()

class Suggestions(BaseModel):
    q: str
    country_code: Optional[str]
    language_code: Optional[str]

class Search(BaseModel):
    q: str
    country_code: Optional[str]
    lat: Optional[int]
    lon: Optional[int]
    country_code: str
    language_code: str

class Location(BaseModel):
    lat: int 
    lon: int

class Item(BaseModel):
    itemId: str
    userId: str
    images: Optional[List[str]]
    title: Optional[str]
    description: Optional[str]
    price: Optional[int] 
    category: Optional[str]  
    subCategory: Optional[str] 
    location: Optional[Point]
    locationDescription: Optional[str] 
    businessName: Optional[str]
    businessDescription: Optional[str]
    phoneNumber: Optional[str]
    inStock: Optional[bool]
    mondayOpeningHours: Optional[str]
    mondayClosingHours:  Optional[str]
    tuesdayOpeningHours:  Optional[str]  
    tuesdayClosingHours:  Optional[str]
    wednesdayOpeningHours:  Optional[str] 
    wednesdayClosingHours:  Optional[str]
    thursdayOpeningHours:  Optional[str]
    thursdayClosingHours:  Optional[str] 
    fridayOpeningHours:  Optional[str]   
    fridayClosingHours:  Optional[str]     
    saturdayOpeningHours:  Optional[str]
    saturdayClosingHours:  Optional[str]
    sundayOpeningHours:  Optional[str] 
    sundayClosingHours:  Optional[str]
    businessProfilePhoto: Optional[str]
    isMondayOpen: Optional[bool]
    isTuesdayOpen: Optional[bool]
    isWednesdayOpen: Optional[bool]
    isThursdayOpen: Optional[bool]
    isFridayOpen: Optional[bool]
    isSaturdayOpen: Optional[bool]
    isSundayOpen: Optional[bool]

es = AsyncElasticsearch()

@app.get('/')
async def home_page():
    return {"Message": "Welcome"}


@app.get('/ping')
async def ping():
    resp = await es.ping()
    return resp

@app.get('/suggestions')
async def search_suggestions(q: str):
    suggestions = []
    search = await es.search(
       index=f'items',
       body={
            "_source": False,
            "suggest": {
                "text": f"{q}",
                "title_suggestion": {
                "completion": {
                    "field": "suggest",
                    "fuzzy": {
                    "fuzziness": 2
                    }
                }
                },
                "title": {
                "term": {
                    "field": "title"
                }
                },
                "title_search_as_you_type": {
                "term": {
                    "field": "title.search_as_you_type"
                }
                },
                "title_2gram_search_as_you_type": {
                "term": {
                    "field": "title.search_as_you_type._2gram"
                }
                },
                "title_3gram_search_as_you_type": {
                "term": {
                    "field": "title.search_as_you_type._3gram"
                }
                },
                "title_term_suggest": {
                "term": {
                    "field": "title.term"
                }
                },
                "title_trigram_suggest": {
                "phrase": {
                    "field": "title.trigram",
                    "size" : 1,
                    "direct_generator" : [ {
                    "field" : "title.trigram",
                    "suggest_mode" : "always"
                    }, {
                    "field" : "title.reverse",
                    "suggest_mode" : "always",
                    "pre_filter" : "reverse",
                    "post_filter" : "reverse"
                    } ]
                }
                },
                "description": {
                "term": {
                    "field": "description"
                }
                },
                "description_search_as_you_type": {
                "term": {
                    "field": "description.search_as_you_type"
                }
                },
                "description_2gram_search_as_you_type": {
                "term": {
                    "field": "description.search_as_you_type._2gram"
                }
                },
                "description_3gram_search_as_you_type": {
                "term": {
                    "field": "description.search_as_you_type._3gram"
                }
                },
                "description_term_suggest": {
                "term": {
                    "field": "description.term"
                }
                },
                "description_trigram_suggest": {
                "phrase": {
                    "field": "description.trigram",
                    "size" : 1,
                    "direct_generator" : [ {
                    "field" : "title.trigram",
                    "suggest_mode" : "always"
                    }, {
                    "field" : "title.reverse",
                    "suggest_mode" : "always",
                    "pre_filter" : "reverse",
                    "post_filter" : "reverse"
                    } ]
                }
                }
            }
        }
    )
    json_suggestions_dump = json.dumps(search)
    json_suggestions = json.loads(json_suggestions_dump)
    all_suggestions = []
    description_suggestions = json_suggestions["suggest"]["description"]
    description_twogram = json_suggestions["suggest"]["description_2gram_search_as_you_type"]
    description_threegram = json_suggestions["suggest"]["description_3gram_search_as_you_type"]
    description_search_as_you_type = json_suggestions["suggest"]["description_search_as_you_type"]
    description_term = json_suggestions["suggest"]["description_term_suggest"]
    description_trigram = json_suggestions["suggest"]["description_trigram_suggest"]
    title = json_suggestions["suggest"]["title"]
    title_threegram = json_suggestions["suggest"]["title_3gram_search_as_you_type"]
    title_twogram = json_suggestions["suggest"]["title_2gram_search_as_you_type"]
    title_search_as_you_type = json_suggestions["suggest"]["title_search_as_you_type"]
    title_suggestion = json_suggestions["suggest"]["title_suggestion"]
    title_term = json_suggestions["suggest"]["title_term_suggest"]
    title_trigram = json_suggestions["suggest"]["title_trigram_suggest"]

    if description_suggestions != None:
        all_suggestions.append(description_suggestions)
    if description_twogram != None:
        all_suggestions.append(description_twogram)
    if description_threegram != None:
        all_suggestions.append(description_threegram)
    if description_search_as_you_type != None:
        all_suggestions.append(description_search_as_you_type)
    if description_term != None:
        all_suggestions.append(description_term)
    if description_trigram != None:
        all_suggestions.append(description_trigram)
    if title != None:
        all_suggestions.append(title)
    if title_threegram != None:
        all_suggestions.append(title_threegram)
    if title_twogram != None:
        all_suggestions.append(title_twogram)
    if title_search_as_you_type != None:
        all_suggestions.append(title_search_as_you_type)
    if title_suggestion != None:
        all_suggestions.append(title_suggestion)
    if title_term != None:
        all_suggestions.append(title_term)
    if title_trigram != None:
        all_suggestions.append(title_trigram)

    for suggestion in all_suggestions:
        for i in suggestion:
            options = i["options"]
            if options != None:
                for option in options:
                    text = option['text']
                    suggestions.append(text)
    suggestions = list(dict.fromkeys(suggestions))
    return {
        'suggestions': suggestions
    }

@app.get('/search')
async def search_detail(
                        q: str,
                        lat: float,
                        lon: float,
                        user_agent: Optional[str] = Header(None), 
                        x_forwarded_for: Optional[str] = Header(None),
                       ):
    # If the user has allowed location access: fetch the data else: use ip geolocation
    parsed_results = []
    if lat and lon:
        results = await es.search(
        index = "items",
        body = {
        "track_scores": True,
        "query": {
            "function_score": {
            "score_mode": "sum", 
            "query": {
                "multi_match": {
                "query": q,
                "fields": [
                    "title^10",
                    "description"
                ],
                "tie_breaker": 0.3
                }
            },
            "functions": [
                {
                "weight": 2.1,
                "gauss": {
                    "location": {
                    "origin": {
                        "lat": lat,
                        "lon": lon
                    },
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
                "location": {
                    "lat": lat,
                    "lon": lon
                },
                "order": "asc",
                "unit": "km",
                "ignore_unmapped": True
            }
            }
        ],
        "aggs": {
            "map_zoom": {
            "geo_bounds": {
                "field": "location"
            }
            }
        }
        },
        size=20,
        )
        for result in results['hits']['hits']:
            source = result['_source']
            sort = result['sort'][0]
            source['distance'] = sort
            parsed_results.append(source)
        bounds = results['aggregations']['map_zoom']['bounds']
        return {
            'items': parsed_results,
            'bounds': bounds,
        }
    else:
        # TODO: fetch the key fron env variables in production
        # key = os.environ.get('IP_GEOLOCATION_KEY')
        key = "a390f3d3aa104eb0b008f3ff8982c055"
        r = requests.get(f"https://api.ipgeolocation.io/ipgeo?apiKey={key}&ip={x_forwarded_for}")
        ip_response = r.json()
        latitude = ip_response['latitude']
        longitude = ip_response['longitude']
        results = await es.search(
        index = f"items",
        body = {
        "track_scores": True,
        "query": {
            "function_score": {
            "score_mode": "sum", 
            "query": {
                "multi_match": {
                "query": q,
                "fields": [
                    "title^10",
                    "description"
                ],
                "tie_breaker": 0.3
                }
            },
            "functions": [
                {
                "weight": 2.1,
                "gauss": {
                    "location": {
                    "origin": {
                        "lat": latitude,
                        "lon": longitude
                    },
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
                "location": {
                    "lat": lat,
                    "lon": longitude
                },
                "order": "asc",
                "unit": "km",
                "ignore_unmapped": True
            }
            }
        ],
        "aggs": {
            "map_zoom": {
            "geo_bounds": {
                "field": "location"
            }
            }
        }
        },
            size=20,
        )
        for result in results['hits']['hits']:
            source = result['_source']
            sort = result['sort'][0]
            source['distance'] = sort
            parsed_results.append(source)
        bounds = results['aggregations']['map_zoom']['bounds']
        return {
            'items': parsed_results,
            'bounds': bounds,
        }

@app.post('/index')
async def index_es(item: Item):
    body = {}
    if item.itemId != None:
        body['itemId'] = item.itemId
    if item.userId != None:
        body['userId'] = item.userId
    if item.images != None:
        body['images'] = item.images
    if item.title != None:
        body['title'] = item.title
    if item.description != None:
        body['description'] = item.description
    if item.price != None:
        body['price'] = item.price
    if item.category != None:
        body['categogry'] = item.category
    if item.subCategory != None:
        body['subCategory'] = item.subCategory
    if item.location != None:
        body['location'] = {
            "lat": item.location["_latitude"],
            "lon": item.location["_longitude"]
        }
    if item.locationDescription != None:
        body['locationDescription'] = item.locationDescription
    if item.businessName != None: 
        body['businessName'] = item.businessName
    if item.businessDescription != None:
        body['businessDescription'] = item.businessDescription
    if item.phoneNumber != None:
        body['phoneNumber'] = item.phoneNumber
    if item.inStock != None:
        body['inStock'] = item.inStock
    if item.mondayOpeningHours != None:
        body['mondayOpeningHours'] = item.mondayOpeningHours
    if item.mondayClosingHours != None:
        body['mondayClosingHours'] = item.mondayClosingHours
    if item.tuesdayOpeningHours != None:
        body['tuesdayOpeningHours'] = item.tuesdayOpeningHours
    if item.tuesdayClosingHours != None:
        body['tuesdayClosingHours'] = item.tuesdayClosingHours
    if item.wednesdayOpeningHours != None:
        body['wednesdayOpeningHours'] = item.wednesdayOpeningHours
    if item.wednesdayClosingHours != None:
        body['wednesdayClosingHours'] = item.wednesdayClosingHours
    if item.thursdayOpeningHours != None:
        body['thursdayOpeningHours'] = item.thursdayOpeningHours
    if item.thursdayClosingHours != None:
        body['thursdayClosingHours'] = item.thursdayClosingHours
    if item.fridayOpeningHours != None:
        body['fridayOpeningHours'] = item.fridayOpeningHours
    if item.fridayClosingHours != None:
        body['fridayClosingHours'] = item.fridayClosingHours
    if item.saturdayOpeningHours != None:
        body['saturdayOpeningHours'] = item.saturdayOpeningHours
    if item.saturdayClosingHours != None:
        body['saturdayClosingHours'] = item.saturdayClosingHours
    if item.sundayOpeningHours != None:
        body['sundayOpeningHours'] = item.sundayOpeningHours
    if item.sundayClosingHours != None:
        body['sundayClosingHours'] = item.sundayClosingHours
    if item.isMondayOpen != None:
        body['isMondayOpen'] = item.isMondayOpen
    if item.isTuesdayOpen != None:
        body['isTuesdayOpen'] = item.isTuesdayOpen
    if item.isWednesdayOpen != None:
        body['isWednesdayOpen'] = item.isWednesdayOpen
    if item.isThursdayOpen != None:
        body['isThursdayOpen'] = item.isThursdayOpen
    if item.isFridayOpen != None:
        body['isFridayOpen'] = item.isFridayOpen
    if item.isSaturdayOpen != None:
        body['isSaturdayOpen'] = item.isSaturdayOpen
    if item.isSundayOpen != None:
        body['isSundayOpen'] = item.isSundayOpen
    if item.businessProfilePhoto != None:
        body['businessProfilePhoto'] = item.businessProfilePhoto

    resp = await es.index( 
        index='items',
        id=item.itemId,
        body=body
    )
    return resp

@app.post('/update')
async def update_index(item: Item):
    body = {}
    if item.itemId != None:
        body['itemId'] = item.itemId
    if item.userId != None:
        body['userId'] = item.userId
    if item.images != None:
        body['images'] = item.images
    if item.title != None:
        body['title'] = item.title
    if item.description != None:
        body['description'] = item.description
    if item.price != None:
        body['price'] = item.price
    if item.category != None:
        body['categogry'] = item.category
    if item.subCategory != None:
        body['subCategory'] = item.subCategory
    if item.location != None:
        body['location'] = {
            "lat": item.location["_latitude"],
            "lon": item.location["_longitude"]
        }
    if item.locationDescription != None:
        body['locationDescription'] = item.locationDescription
    if item.businessName != None: 
        body['businessName'] = item.businessName
    if item.businessDescription != None:
        body['businessDescription'] = item.businessDescription
    if item.phoneNumber != None:
        body['phoneNumber'] = item.phoneNumber
    if item.inStock != None:
        body['inStock'] = item.inStock
    if item.mondayOpeningHours != None:
        body['mondayOpeningHours'] = item.mondayOpeningHours
    if item.mondayClosingHours != None:
        body['mondayClosingHours'] = item.mondayClosingHours
    if item.tuesdayOpeningHours != None:
        body['tuesdayOpeningHours'] = item.tuesdayOpeningHours
    if item.tuesdayClosingHours != None:
        body['tuesdayClosingHours'] = item.tuesdayClosingHours
    if item.wednesdayOpeningHours != None:
        body['wednesdayOpeningHours'] = item.wednesdayOpeningHours
    if item.wednesdayClosingHours != None:
        body['wednesdayClosingHours'] = item.wednesdayClosingHours
    if item.thursdayOpeningHours != None:
        body['thursdayOpeningHours'] = item.thursdayOpeningHours
    if item.thursdayClosingHours != None:
        body['thursdayClosingHours'] = item.thursdayClosingHours
    if item.fridayOpeningHours != None:
        body['fridayOpeningHours'] = item.fridayOpeningHours
    if item.fridayClosingHours != None:
        body['fridayClosingHours'] = item.fridayClosingHours
    if item.saturdayOpeningHours != None:
        body['saturdayOpeningHours'] = item.saturdayOpeningHours
    if item.saturdayClosingHours != None:
        body['saturdayClosingHours'] = item.saturdayClosingHours
    if item.sundayOpeningHours != None:
        body['sundayOpeningHours'] = item.sundayOpeningHours
    if item.sundayClosingHours != None:
        body['sundayClosingHours'] = item.sundayClosingHours
    if item.businessPhotos != None:
        body['businessProfilePhoto'] = item.businessProfilePhoto
    if item.isMondayOpen != None:
        body['isMondayOpen'] = item.isMondayOpen
    if item.isTuesdayOpen != None:
        body['isTuesdayOpen'] = item.isTuesdayOpen
    if item.isWednesdayOpen != None:
        body['isWednesdayOpen'] = item.isWednesdayOpen
    if item.isThursdayOpen != None:
        body['isThursdayOpen'] = item.isThursdayOpen
    if item.isFridayOpen != None:
        body['isFridayOpen'] = item.isFridayOpen
    if item.isSaturdayOpen != None:
        body['isSaturdayOpen'] = item.isSaturdayOpen
    if item.isSundayOpen != None:
        body['isSundayOpen'] = item.isSundayOpen
    
    resp = await es.update(
        index='items',
        id=item.itemId,
        body={
            "doc": body
        }
    )
    return resp

class DeleteModel(BaseModel):
    itemId: str
    admin_email: str

@app.post('/delete')
async def delete_document(item: DeleteModel):
    # if item.admin_email == os.environ.get('FIREBASE_AUTH'):
    if item.admin_email == 'admin@localhost.com':
        await es.delete(
            index='items',
            id=item.itemId
        )
    return {'Message': 'Successful Delete'}

@app.get('/recommendations')
async def recommendations(deviceId: str, lat: float, lon: float):
    # Get recommedations for a specific device
    return {'Message': "Recommendations"}

@app.post('/item-viewstream', status_code=200)
async def item_viewstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f" Bad Request: {msg}"
    
    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f"Bad Request: {msg}"
    
    pubsub_message = envelope["message"]

    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        body = base64.b64decode(pubsub_message["data"]).decode("utf-8")
        json_body = json.loads(body)
        viewId = json_body["viewId"]
        resp = await item_viewstream_index(viewId, json_body)
        return resp

    return ""

async def item_viewstream_index(id, body):
    resp = await es.index( 
        index='item-viewstream',
        id=id,
        body=body
    )
    return resp    

@app.post('/merchant-viewstream')
# async def item

@app.on_event("shutdown")
async def app_shutdown():
    await es.close()

