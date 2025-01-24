import os
from fastapi import FastAPI
from dotenv import load_dotenv
from api.jobs_routes import router as jobs_router  # Import du routeur

# Charger les variables d'environnement depuis .env
load_dotenv()

# Instancier l'application FastAPI
app = FastAPI()

# Monter les routes du fichier jobs_routes.py
app.include_router(jobs_router, prefix="/jobs", tags=["Jobs"])


