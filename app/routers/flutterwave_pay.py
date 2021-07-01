from fastapi import APIRouter
from typing import Optional
from pydantic import BaseModel
import uuid
import json
import requests

router = APIRouter()

class CustomerInfo(BaseModel):
    id: Optional[str] = None
    email: Optional[str] = None
    phonenumber: Optional[str] = None
    name: Optional[str] = None

class PaymentDetails(BaseModel):
    payment_options: Optional[str] = None
    amount: Optional[float] = None
    customer_info: Optional[CustomerInfo] = None

@router.post("/pay", tags=["flutterwave_pay"])
async def custom_pay(details: PaymentDetails):
    print(details)
    flutterwave_url = "https://api.flutterwave.com/v3/payments"
    ref = uuid.uuid4()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': "Bearer FLWSECK-dcbb904f831b7a376e244ba3ff4c599c-X",
    }
    data = {
      "tx_ref": f"beammart-{ref}",
      "amount": details.amount,
      "currency": "KES",
      "redirect_url": "https://www.beammart.app/",
      "payment_options": details.payment_options,
      "customer": {
        "email": details.customer_info.email,
        "phonenumber": details.customer_info.phonenumber,
        "name": details.customer_info.name,
      },
      "customizations": {
        "title": "Beammart Corporation",
        "description": "Purchase Tokens",
        "logo": "https://assets.piedpiper.com/logo.png"
      },
      "meta": {
        "customer_id" : details.customer_info.id
      }
    }
    json_data = json.dumps(data)
    r = requests.post(flutterwave_url, data = json_data, headers=headers)
    print(r.status_code)
    return r.json()
