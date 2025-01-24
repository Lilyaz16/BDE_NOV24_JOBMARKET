import requests
import os
import time
from dotenv import load_dotenv




#Extraction des données France Travail
load_dotenv()

base_url = "https://entreprise.pole-emploi.fr/"
token_url = f"{base_url}connexion/oauth2/access_token?realm=/partenaire"
api_url = "https://api.pole-emploi.io/partenaire/offresdemploi/v2/offres/search"



# Récupération du token 
def post_token(client_id, secret_key):
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": secret_key,
        "scope": "api_offresdemploiv2 o2dsoffre"
    }
    response = requests.post(token_url, headers=headers, data=data)
    if response.status_code == 200:
        print("token ok")
        return response.json()["access_token"]
    else:
        raise Exception(f"Erreur d'authentification: {response.status_code}, {response.text}")


#
def extract_ft_data(access_token, motsCles=None, codePostal=None, max_results=10000):
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    results = []
    range_step = 100

    api_url = "https://api.pole-emploi.io/partenaire/offresdemploi/v2/offres/search" 

    for start in range(0, max_results, range_step):
        params = {
            "motsCles": motsCles,
            "codePostal": codePostal,
            "range": f"{start}-{start + range_step - 1}"
        }
        response = requests.get(api_url, headers=headers, params=params)

        if response.status_code in [200, 206]:  # 206 = Partial Content
            # Au lieu de "results(response.json().get("resultats", []))" on met :
            results.extend(response.json().get("resultats", []))
        else:
            # Si le code n'est pas 200 ou 206, on arrête la boucle
            break

    # Vérification si 'results' est vide
    if not results:  
        print("Aucun résultat n'a été trouvé ou la réponse est vide.")
    else:
        print(f"{len(results)} résultats récupérés.")

    return results




#Extraction des données Adzuna

COUNTRY = "fr"
params = {
    "app_id": os.environ.get("APP_ID"),
    "app_key": os.environ.get("APP_KEY"),
    "results_per_page": 50,
    "what": "",
    "content-type": "application/json"
}
api_url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search"


#Récupérer le résultat de la requête requests

params = {
    "app_id": os.getenv("APP_ID"),  # Charge APP_ID depuis .env
    "app_key": os.getenv("APP_KEY"),  # Charge APP_KEY depuis .env
    "results_per_page": 50,
    "what": "",
    "content-type": "application/json"
}



def download_page(page):
    """Télécharge une page de résultats depuis l'API."""
    
    result = requests.get(f"{api_url}/{page}", params=params)
    if result.status_code == 200:
        print("La page", page, "a bien été téléchargée")
        return result.json()['results']
    else:
        print("Le téléchargement a échoué avec le code :", result.status_code)
        print("Réponse : ", result.text)
        time.sleep(1)
        return []

# Fonction pour télécharger plusieurs pages
def download_all(max_pages=100):
    """Télécharge un nombre défini de pages de résultats."""
    results = []
    for page in range(1, max_pages + 1):
        results += download_page(page)
    return results


# Fonction principale d'extraction
def extract_adzuna_data(max_pages=101):
    """Extrait les données depuis Adzuna avec un nombre configurable de pages."""
    print("Démarrage de l'extraction des données depuis Adzuna...")
    all_results = download_all(max_pages=max_pages)
    print(f"Extraction terminée. Nombre total d'offres récupérées : {len(all_results)}")
    return all_results



#Extraction des données Indeed
