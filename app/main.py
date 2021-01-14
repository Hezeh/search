import os

from fastapi import FastAPI, Header
from elasticsearch import AsyncElasticsearch, Urllib3HttpConnection
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

    es = AsyncElasticsearch([f'https://{user}:{secret}@{host}'],  
    sniff_on_start=True,
    sniff_on_connection_fail=True,
    sniffer_timeout=60,
    maxsize=256,
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False
    )
else:
    es = AsyncElasticsearch()

@app.get('/')
async def home_page():
    return {"Message": "Welcome"}


@app.get('/ping')
async def ping():
    resp = await es.ping()
    return {
        'response': resp,
        'host': host,
        'user': user,
        'secret': secret,
    }

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

app.get('/gram')
async def ngram(q: str):
    suggestions = []
    search_results = es.search(
        index='items-ken-en',
        body={
            "query": {
                "match_phrase_prefix": {
                    "title": f"{q}"
                }
            }
        }
    )
    # Extract the suggestions
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

# "analysis": {
#  "filter": {
#  "autocomplete_filter": {
#  "type": "edge_ngram",
#  "min_gram": 1,
#  "max_gram": 20
#  }
#  },
#  "analyzer": {
#  "autocomplete": {
#  "type": "custom",
#  "tokenizer": "standard",
#  "filter": [
#  "lowercase",
#  "autocomplete_filter"
#  ]
#  }
#  }
#  }
