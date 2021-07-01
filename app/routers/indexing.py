from fastapi import APIRouter, Request
import json
import base64
from .app_variables import es

router = APIRouter()

async def item_index(id, body):
    resp = await es.index(index="items", id=id, body=body)
    return resp


@router.post("/index", tags=["indexing"])
async def index_es(request: Request):
    envelope = await request.body()
    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    id = json_payload["itemId"]
    if "location" in json_payload:
        latitude = json_payload["location"]["_latitude"]
        longitude = json_payload["location"]["_longitude"]
        json_payload["location"] = {"lat": latitude, "lon": longitude}
    resp = await item_index(id, json_payload)
    return resp


async def item_update(id, body):
    resp = await es.update(index="items", id=id, body={"doc": body})
    return resp


@router.post("/update", tags=["indexing"])
async def update_index(request: Request):
    envelope = await request.body()
    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    id = json_payload["itemId"]
    latitude = json_payload["location"]["_latitude"]
    longitude = json_payload["location"]["_longitude"]
    json_payload["location"] = {"lat": latitude, "lon": longitude}
    exists = await es.exists(index="items", id=id)
    if exists:
        resp = await item_update(id, json_payload)
        return resp
    else:
        resp = await item_index(id, json_payload)
        return resp

async def item_delete(id):
    # TODO: If doc exists then delete else just return
    resp = await es.delete(index="items", id=id)
    return resp


@router.post("/delete", tags=["indexing"])
async def delete_document(request: Request):
    envelope = await request.body()
    pubsub_message = json.loads(envelope.decode("utf-8"))
    payload = base64.b64decode(pubsub_message["message"]["data"])
    json_payload = json.loads(payload)
    id = json_payload["itemId"]
    exists = await es.exists(index="items", id=id)
    if exists:
        resp = await item_delete(id)
        return resp
    else:
        return {}