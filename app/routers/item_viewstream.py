from fastapi import APIRouter, Response, Request, status
import json
import base64
from .app_variables import es

router = APIRouter()

@router.post("/item-viewstream", status_code=200, tags=["item_viewstream"])
async def item_viewstream(request: Request, response: Response):
    envelope = await request.body()
    lat = None
    lon = None
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return f" Bad Request: {msg}"

    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    viewId = json_payload["viewId"]
    if "lat" in json_payload:
        lat = json_payload["lat"] 
    if "lon" in json_payload:
        lon = json_payload["lon"]
    if lat != None and lon != None:
        json_payload["location"] = {
            "lat": lat,
            "lon": lon,
        }
    resp = await item_viewstream_index(viewId, json_payload)
    return resp


async def item_viewstream_index(id, body):
    resp = await es.index(index="item-viewstream", id=id, body=body)
    return resp
