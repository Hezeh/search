from fastapi import APIRouter
from .app_variables import es

router = APIRouter()

@router.get('/impressions/analytics/{merchant_id}', tags=["merchant_impressions"])
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
