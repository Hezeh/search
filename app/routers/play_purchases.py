from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build

router = APIRouter()

class PurchaseModel(BaseModel):
    packageName: Optional[str]
    token: Optional[str]
    subscriptionId: Optional[str]

@router.post('/purchases/subscriptions', tags=["play_purchases"])
async def verify_purchase(purchase: PurchaseModel):
    credentials = service_account.Credentials.from_service_account_file("service_account.json")
    scoped_credentials = credentials.with_scopes(
    ['https://www.googleapis.com/auth/androidpublisher'])
    # credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/androidpublisher'])
    service = build("androidpublisher", "v3", credentials=scoped_credentials)
    # credentials = ServiceAccountCredentials.from_json_keyfile_name(
    #   'service-account-abcdef123456.json',
    # scopes='https://www.googleapis.com/auth/tasks')

    # # Create an httplib2.Http object to handle our HTTP requests and authorize
    # # it with the Credentials.
    # http = httplib2.Http()
    # http = credentials.authorize(http)

    # service = build("tasks", "v1", http=http)
    #Use the token your API got from the app to verify the purchase
    # result = service.purchases().subscriptions().get(packageName=purchase.packageName, subscriptionId=purchase.subscriptionId, token=purchase.token).execute()
    # key = 'AIzaSyDx3sQEe0FwOcrUxthdrYeTO-CuZfa1nrc'
    # google_url = f'https://androidpublisher.googleapis.com/androidpublisher/v3/applications/{purchase.packageName}/purchases/subscriptions/{purchase.subscriptionId}/tokens/{purchase.token}?key={key}'
    # async with aiohttp.ClientSession() as session:
    #     async with session.get(google_url, headers={
    #         'Authorization': 'Bearer 207309330467-jcfa7l875gfh46s0thqo5atpkagartoi.apps.googleusercontent.com',
    #         'Accept': 'application/json'
    #     }) as response:

    #         print("Status:", response.status)
    #         print("Content-type:", response.headers['content-type'])

    #         resp_json = await response.json()
    #         print(resp_json)
    # print(result)
    return {}