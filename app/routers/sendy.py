from fastapi import APIRouter
import requests
import json

router = APIRouter()

@router.post("/sendy", tags=["Sendy"])
async def sendy_request():
    sendy_api = "https://apitest.sendyit.com/v1/##request"
    values = {
    "command": "request",
    "data": {
      "api_key": "H42bgpNvPPChZ8Yhh8NO",
      "api_username": "beammartcorporation",
      "vendor_type": 1,
      "rider_phone":"0728561783",
      "from": {
        "from_name": "Westlands",
        "from_lat": -1.2681032,
        "from_long": 36.8012739,
        "from_description": ""
      },
      "to": {
        "to_name": "Mamlaka",
        "to_lat": -1.2891308,
        "to_long": 36.8187401,
        "to_description": ""
      },
      "recepient": {
        "recepient_name": "Hezekiah",
        "recepient_phone": "0709779779",
        "recepient_email": "hezekiahmaina3@gmail.com",
        "recepient_notes": "recepient specific Notes",
        "recepient_notify": True
      },
      "sender": {
        "sender_name": "Beammart Corporation",
        "sender_phone": "0709 779 779",
        "sender_email": "hezekiah@beammart.app",
        "sender_notes": "Sender specific notes",
        "sender_notify": True
      },
      "delivery_details": {
        "pick_up_date": "2021-06-25 12:12:12",
        "collect_payment": {
          "status": True,
          "pay_method": 0,
          "amount": 10
        },
        "carrier_type": 1,
        "return": False,
        "note": " Sample note",
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
            "item_name": "laptop"
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
