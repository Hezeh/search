from fastapi import APIRouter
from .app_variables import es

router = APIRouter()

@router.get('/item/clicks/analytics/{item_id}', tags=["item_clicks"])
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