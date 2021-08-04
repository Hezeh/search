from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from .app_variables import db

import africastalking

username = "beammart"
api_key = "dd639f0dda4df27cf951cd2e7b4476c24232886334fb9e8e6de4a114584d9660"      

router = APIRouter()

class User(BaseModel):
    userId: Optional[str]
    listId: Optional[str]
    message: Optional[str]

@router.post("/send-message", tags=["AT"])
async def at_main(user: User):
    userId = user.userId
    listId = user.listId
    message = user.message
    # Retrieve messages from firestore
    users_ref = db.collection(u'contacts').document(f"{userId}").collection("contact-list").document(f"{listId}").collection("contact")
    docs = users_ref.stream()
    phone_numbers = []

    for doc in docs:
        # print(f'{doc.id} => {doc.to_dict()}')
        doc_data = doc.to_dict()
        phone_number = doc_data["phoneNumber"]
        phone_numbers.append(phone_number)

    # Send Message using AT
    send_message(message, phone_numbers)
    return { 
        # "phone_numbers": phone_numbers,
    }

def send_message(message, phone_numbers):
    africastalking.initialize(username, api_key)
    # Initialize a service e.g. SMS
    sms = africastalking.SMS

    # Use the service synchronously
    response = sms.send(f"{message}", phone_numbers)
    # print(response)
    return response