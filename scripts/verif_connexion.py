"""
Script 00 - Vérification de la connexion MongoDB
Lance ce script en premier pour t'assurer que tout fonctionne.
"""

from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "banques_senegal")


def verifier_connexion():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.server_info()
        print(f"✅  Connexion MongoDB OK  →  {MONGO_URI}")

        db = client[DB_NAME]
        collections = db.list_collection_names()
        if collections:
            print(f"\n📋  Collections existantes dans '{DB_NAME}' :")
            for col in collections:
                n = db[col].count_documents({})
                print(f"    - {col} : {n} documents")
        else:
            print(f"\n📭  La base '{DB_NAME}' est vide (normal si ingestion pas encore faite)")

        client.close()

    except Exception as e:
        print(f"❌  Impossible de se connecter à MongoDB : {e}")
        print("\n💡  Vérifie que MongoDB est bien démarré :")
        print("    Windows : services.msc  →  MongoDB")
        print("    macOS   : brew services start mongodb-community")
        print("    Linux   : sudo systemctl start mongod")


if __name__ == "__main__":
    verifier_connexion()