from fastapi import APIRouter
from .app_variables import es

router = APIRouter()

@router.get('/item/impressions/analytics/{item_id}', tags=["item_impressions"])
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