from collections import defaultdict
from . import main

async def recommendations():
    resp = await main.es.search(
        index="items",
        body={
            "query": {
                "bool": {
                    "should": [
                        {"terms": {"category.keyword": ["Food"]}},
                        {
                            "terms": {
                                "subCategory.keyword": [
                                    "Snacks",
                                    "Skateboards and Skates"
                                ]
                            }
                        },
                    ]
                }
            },
            "aggs": {
                "categories": {
                    "terms": {"field": "category.keyword", "size": 10},
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
    recs = defaultdict(list)
    buckets = resp["aggregations"]["categories"]["buckets"]
    for bucket in buckets:
        category = bucket["key"]
        if len(docs) != 0:
            for doc in docs:
                doc_category = doc["_source"]["category"]
                if doc_category == category:
                    recs[f"{category}"].append(doc)
    return recs