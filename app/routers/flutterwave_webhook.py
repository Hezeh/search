from fastapi import APIRouter, Request
import json
import requests
from .app_variables import db, transaction
from firebase_admin import firestore

router = APIRouter()

@firestore.transactional
def update_profile_in_transaction(transaction, profile_ref, amount):
    snapshot = profile_ref.get(transaction=transaction)
    transaction.update(profile_ref, {
        u'tokensBalance': snapshot.get(u'tokensBalance') + amount
    })

@router.post("/wave-webhook", tags=["flutterwave_webhook"])
async def main(request: Request):
    envelope = await request.body()
    data = json.loads(envelope.decode("utf-8"))
    headers = {
        'Content-Type': 'application/json',
        'Authorization': "Bearer FLWSECK-dcbb904f831b7a376e244ba3ff4c599c-X",
    }
    if data is not None:
        if data['event'] == "charge.completed":
            event_data = data['data']
            status = event_data['status']
            if status == "successful":
                payment_id = event_data['id']
                # Verify transaction
                r = requests.get(f'https://api.flutterwave.com/v3/transactions/{payment_id}/verify', headers=headers)
                if r.status_code == 200:
                    json_response = r.json()
                    transaction_data = json_response["data"]
                    amount = transaction_data["amount"]
                    customer_data = transaction_data['customer']
                    meta = transaction_data['meta']
                    customer_email = customer_data['email']
                    customer_id = meta['customer_id']
                    if customer_id != None:
                        doc_ref = db.collection(u'profile').document(f'{customer_id}')
                        doc = doc_ref.get()
                        if doc.exists:
                            # Check the amount and update the db
                            if amount == 200:
                                # Add 200 tokens to db & send an email to customer
                                update_profile_in_transaction(transaction, doc_ref, 200)
                            elif amount == 500:
                                update_profile_in_transaction(transaction, doc_ref, 500)
                            elif amount == 1000:
                                update_profile_in_transaction(transaction, doc_ref, 1000)
                            elif amount == 2500:
                                update_profile_in_transaction(transaction, doc_ref, 2500)
                            elif amount == 5000:
                                update_profile_in_transaction(transaction, doc_ref, 5000)
                            elif amount == 10000:
                                update_profile_in_transaction(transaction, doc_ref, 10000)
                        else:
                            print(u'No such document!')
                        
                
        return {}
    else:
        return {
            "message": "An error occurred"
        }
