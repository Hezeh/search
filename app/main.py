import os

from fastapi import FastAPI, Header
from elasticsearch import AsyncElasticsearch
from typing import Optional
import requests
from pydantic import BaseModel
import json

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


# Whether the environ is dev or prod
prod = os.environ.get('ENV')
if prod == 'prod':
    user = os.environ.get("ELASTIC_USER")
    secret = os.environ.get("ELASTIC_SECRET")
    host = os.environ.get("ELASTIC_HOST")
    es = AsyncElasticsearch([f'https://{user}:{secret}@{host}'])
else:
    es = AsyncElasticsearch()


@app.get('/')
async def home_page():
    return {"Message": "Welcome"}


@app.get('/ping')
async def ping():
    resp = await es.ping()
    return resp

@app.get('/suggestions')
async def search_suggestions(q: str, country_code: str, language_code: str):
    suggestions = []
    search = await es.search(
       index=f'items-{country_code}-{language_code}',
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
    # print(f"json_suggestions {json_suggestions}")
    description_suggestions = json_suggestions["suggest"]["description"]
    for desc_suggestion in description_suggestions:
        options = desc_suggestion['options']
        if options != None:
            for option in options:
                text = option['text']
                suggestions.append(text)
    print(suggestions)
    return suggestions

@app.get('/search')
async def search_detail(
                        search_query: Search,
                        user_agent: Optional[str] = Header(None), 
                        x_forwarded_for: Optional[str] = Header(None),
                       ):
    # If the user has allowed ip access: fetch the data else: use ip geolocation
    if search_query.lat and search_query.lon:
        response = await es.search(
        index = f"items-{search_query.country_code}-{search_query.language_code}",
        body = {
            "query": {
            "multi_match": {
                "query": search_query.q,
                "type": "best_fields",
                "fields": ["title", "description"],
                }
            }
            },
        size=20,
        )
        return response
    else:
        # TODO: fetch the key fron env variables in production
        # key = os.environ.get('IP_GEOLOCATION_KEY')
        key = "a390f3d3aa104eb0b008f3ff8982c055"
        r = requests.get(f"https://api.ipgeolocation.io/ipgeo?apiKey={key}&ip={x_forwarded_for}")
        ip_response = r.json()
        latitude = ip_response['latitude']
        longitude = ip_response['longitude']
        search = await es.search(
            index = f"items-{search_query.country_code}-{search_query.language_code}",
            body = {
                "query": {
                    "multi_match": {
                        "query": search_query.query,
                        "type": "best_fields",
                        "fields": ["title", "description"],
                    }
                }
            },
            size=20,
        )
        return search

@app.on_event("shutdown")
async def app_shutdown():
    await es.close()

# Boosting
# A boost value of between 1 and 15 is recommended
# {
#  "query": {
#  "bool": {
#  "should": [
#  { "match": {
#  "title": {
#  "query": "War and Peace",
#  "boost": 2
#  }}},
#  { "match": {
#  "author": {
#  "query": "Leo Tolstoy",
#  "boost": 2
#  }}},
#  { "bool": {
#  "should": [
#  { "match": { "translator": "Constance Garnett" }},
#  { "match": { "translator": "Louise Maude" }}
#  ]
#  }}
#  ]
#  }
#  }
# }

# # Disjunction Max Query
# # simply means return documents that match any of these queries, and return the score of
# # the best matching query
#  {
#  "query": {
#  "dis_max": {
#  "queries": [
#  { "match": { "title": "Brown fox" }},
#  { "match": { "body": "Brown fox" }}
#  ]
#  }
#  }
# }

# # Recommended tie breaker is 0 to 1 but closest to 0
#  "query": {
#  "dis_max": {
#  "queries": [
#  { "match": { "title": "Quick pets" }},
#  { "match": { "body": "Quick pets" }}
#  ],
#  "tie_breaker": 0.3
#  }
#  }
# }

# # Using multi_match
# {
#  "dis_max": {
#  "queries": [
#  {
#  "match": {
#  "title": {
#  "query": "Quick brown fox",
#  "minimum_should_match": "30%"
#  }
#  }
#  },
#  {
#  "match": {
#  "body": {
#  "query": "Quick brown fox",
#  "minimum_should_match": "30%"
#  }
#  }
#  },
#  ],
#  "tie_breaker": 0.3
#  }
# }

# # The above query can simply be written as 
# {
#  "multi_match": {
#  "query": "Quick brown fox",
#  "type": "best_fields",
#  "fields": [ "title", "body" ],
#  "tie_breaker": 0.3,
#  "minimum_should_match": "30%"
#  }
# }

# # Boosting individual Fields in multi_match queries
# {
#  "multi_match": {
#  "query": "Quick brown fox",
#  "fields": [ "*_title", "chapter_title^2" ]
#  }
# }

# # Search-as-you-type
# {
#     "query": {
#         "match_phrase_prefix": {
#             "title": {
#                 "query": query
#             }
#         }
#     }
# }