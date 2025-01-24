import os
import subprocess
from fastapi import APIRouter
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Récupérer les chemins des scripts depuis le fichier .env
DATA_EXTRACTION_PATH = os.getenv("DATA_EXTRACTION_PATH")
DATA_TRANSFORM_PATH = os.getenv("DATA_TRANSFORM_PATH")
LOAD_SNOWFLAKE_PATH = os.getenv("LOAD_SNOWFLAKE_PATH")
STREAMLIT_APP_PATH = os.getenv("STREAMLIT_APP_PATH")

# Créer un routeur pour les jobs
router = APIRouter()

@router.get("/run_extraction")
def run_extraction():
    subprocess.run(["python", DATA_EXTRACTION_PATH])
    return {"status": "Extraction completed"}

@router.get("/run_transform")
def run_transform():
    subprocess.run(["python", DATA_TRANSFORM_PATH])
    return {"status": "Extraction completed"}

@router.get("/load_to_snowflake")
def load_to_snowflake():
    subprocess.run(["python", LOAD_SNOWFLAKE_PATH])
    return {"status": "Data loaded to Snowflake"}

print("STREAMLIT_APP_PATH:", os.getenv("STREAMLIT_APP_PATH"))
@router.get("/run_streamlit")

def run_streamlit():
    if not STREAMLIT_APP_PATH or not os.path.isfile(STREAMLIT_APP_PATH):
        return {"error": f"Le fichier {STREAMLIT_APP_PATH} est introuvable."}
    try:
        subprocess.run(["streamlit", "run", STREAMLIT_APP_PATH])
        return {"status": "Streamlit app started"}
    except Exception as e:
        return {"error": str(e)}

