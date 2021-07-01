from fastapi import APIRouter, Response, Request, status
import json 
import base64
from .app_variables import es

router = APIRouter()

@router.post('/clickstream', status_code=200, tags=["clickstream"])
async def serp_clickstream(request: Request, response: Response):
    envelope = await request.body()
    if not envelope:
        msg = "no Pub/Sub message received"
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f"Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    eventId = json_payload['eventId']
    if "lat" in json_payload:
        lat = json_payload["lat"] 
    if "lon" in json_payload:
        lon = json_payload["lon"]
    if lat != None and lon != None:
        json_payload["location"] = {
            "lat": lat,
            "lon": lon,
        }
    await indexing_func("clickstream", eventId, json_payload)
    return {}

async def indexing_func(index, id, body):
    resp = await es.index(index=index, id=id, body=body)
    return resp