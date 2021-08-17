from fastapi import APIRouter
from .app_variables import es

router = APIRouter()

@router.get('/merchant/{merchant_id}', tags=["merchant"])
async def merchant_items(merchant_id: str):
    parsed_results = []
    resp = await es.search(  
        index='items',
        size=250,
        body={
            "query": {
                "match": {
                    "userId.keyword": merchant_id
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
