from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel
import requests
import json

router = APIRouter()

class DeliveryDetails(BaseModel):
  item_name: Optional[str]
  amount: Optional[str]
  pickup_date: Optional[str]
  note: Optional[str]

class Recipient(BaseModel):
    recipient_name: Optional[str]
    recipient_phone: Optional[str]
    recipient_email: Optional[str]
    recipient_notes: Optional[str]
    recipient_notify: Optional[bool]

class Merchant(BaseModel):
    sender_name: Optional[str]
    sender_phone: Optional[str]
    sender_email: Optional[str]
    sender_notes: Optional[str]
    sender_notify: Optional[bool]

class CustomerAddress(BaseModel):
    address_name: Optional[str]
    address_lat: Optional[float]
    address_lon: Optional[float]
    address_description: Optional[str]

class MerchantAddress(BaseModel):
    address_name: Optional[str]
    address_lat: Optional[float]
    address_lon: Optional[float]
    address_description: Optional[str]

@router.post('/get-rider', tag=['logistics'])
async def get_rider(recipient: Recipient, 
                    merchant: Merchant, 
                    customer_address: CustomerAddress, 
                    merchant_address: MerchantAddress,
                    order_details: DeliveryDetails,
                ):

    sendy_api = "https://apitest.sendyit.com/v1/##request"
    sendy_api_key = "H42bgpNvPPChZ8Yhh8NO"
    sendy_api_username = "beammartcorporation"
    values = {
    "command": "request",
    "data": {
      "api_key": sendy_api_key,
      "api_username": sendy_api_username,
      "vendor_type": 1,
      "from": {
        "from_name": merchant_address.address_name,
        "from_lat": merchant_address.address_lat,
        "from_long": merchant_address.address_lon,
        "from_description": merchant_address.address_description,
      },
      "to": {
        "to_name": customer_address.address_name,
        "to_lat": customer_address.address_lat,
        "to_long": customer_address.address_lon,
        "to_description": customer_address.address_description
      },
      "recepient": {
        "recepient_name": recipient.recipient_name,
        "recepient_phone": recipient.recipient_phone,
        "recepient_email": recipient.recipient_email,
        "recepient_notes": recipient.recipient_notes,
        "recepient_notify": recipient.recipient_notify
      },
      "sender": {
        "sender_name": merchant.sender_name,
        "sender_phone": merchant.sender_phone,
        "sender_email": merchant.sender_email,
        "sender_notes": merchant.sender_notes,
        "sender_notify": merchant.sender_notify
      },
      "delivery_details": {
        "pick_up_date": order_details.pickup_date,
        "collect_payment": {
          "status": True,
          "pay_method": 0,
          "amount": order_details.amount,
        },
        "carrier_type": 1,
        "return": False,
        "note": "",
        "note_status": True,
        "request_type": "delivery",
        "order_type": "ondemand_delivery",
        "ecommerce_order": False,
        "express": False,
        "skew": 1,
        "package_size": [
          {
            "weight": 20,
            "height": 10,
            "width": 200,
            "length": 30,
            "item_name": order_details.item_name
          }
        ]
      }
    },
    "request_token_id": "request_token_id"
  }
    headers = {
        'Content-Type': 'application/json'
    }
    json_data = json.dumps(values)
    r = requests.post(sendy_api, data = json_data, headers=headers)
    print(r.status_code)
    return r.json()