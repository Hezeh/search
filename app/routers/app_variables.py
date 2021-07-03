from elasticsearch._async.client import AsyncElasticsearch
from firebase_admin import credentials, firestore
import firebase_admin


es = AsyncElasticsearch()

project_id = 'beammart'
# Use the application default credentials
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
  'projectId': project_id,
})

# Use a service account
# cred = credentials.Certificate('/home/hezekiah/firestore-beammart.json')
# firebase_admin.initialize_app(cred)

db = firestore.client()
transaction = db.transaction()