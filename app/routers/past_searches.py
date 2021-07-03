from fastapi import APIRouter, Header
from pydantic import BaseModel
from typing import Optional
import uuid
from .app_variables import es

router = APIRouter()

class PastSearch(BaseModel):
    deviceId: Optional[str]
    userId: Optional[str]
    query: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    timestamp: Optional[str]

@router.post("/searches", tags=["past_searches"])
async def past_searches(past_search: PastSearch, x_forwarded_for: Optional[str] = Header(None),):
    id = uuid.uuid4()
    body = {}
    if past_search.deviceId != None:
        body["deviceId"] = past_search.deviceId
    if past_search.userId != None:
        body["userId"] = past_search.userId
    if past_search.query != None:
        body["query"] = past_search.query
    if past_search.timestamp != None:
        body["timestamp"] = past_search.timestamp
    if past_search.lat != None and past_search.lon:
        body["location"] = {
            "lat": past_search.lat,
            "lon": past_search.lon
        }
    body["ipAddress"] = x_forwarded_for
    resp = await es.index(index="past-searches", id=id, body=body)
    return resp
