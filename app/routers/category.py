from fastapi import APIRouter
from typing import Optional
from .app_variables import es

router = APIRouter()

@router.get('/category/{category_name}', tags=["category"])
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