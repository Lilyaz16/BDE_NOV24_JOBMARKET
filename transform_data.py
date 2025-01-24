import pandas as pd
import re
import requests
import os
import time
from datetime import datetime

# Import des fonctions d'extraction
from extract_data import extract_ft_data, extract_adzuna_data, post_token
  

# Transformation pour Adzuna
def transform_adzuna(results):
    """Transforme les données Adzuna en un DataFrame."""
    job_list = [
        {
            "origin": "Adzuna",
            "id": job.get("id"),
            "title": job.get("title"),
            "publication_date": job.get("created"),
            "company": job.get("company", {}).get("display_name"),
            "location": job.get("location", {}).get("display_name", "N/A"),  # Valeur par défaut "N/A" si absente
            "city": None,
            "code_postal": job.get("location", {}).get("display_name", "N/A"),  # Même gestion ici
            "salary": job.get("salary_min"),
            "contract_type": job.get("contract_type"),
            "category": job.get("category", {}).get("label"),
            "description": job.get("description"),
        }
        for job in results
    ]
    df_adzuna = pd.DataFrame(job_list)
    df_adzuna['city'] = df_adzuna['location'].str.split(',').str[0].str.strip()
    df_adzuna['salary'] = df_adzuna['salary'].apply(lambda x: x / 12 if isinstance(x, (int, float)) and x >= 10000 else x)
    return df_adzuna
    

# Transformation pour France Travail
def transform_france_travail(job_list):
    """Transforme les données France Travail en un DataFrame."""
    df_ft = pd.DataFrame([ 
        {
            "origin": "France Travail",
            "id": job.get("id"),
            "title": job.get("intitule"),
            "publication_date": job.get("dateCreation"),
            "company": job.get("entreprise", {}).get("nom"),
            "location": job.get("lieuTravail", {}).get("libelle", "N/A"),  # Valeur par défaut "N/A" si absente
            "city": None,
            "code_postal": job.get("lieuTravail", {}).get("codePostal", "N/A"),  
            "salary": job.get("salaire", {}).get("libelle", "N/A"),  
            "contract_type": job.get("typeContratLibelle", "N/A"),  
            "category": job.get("secteurActiviteLibelle", "N/A"),  
            "description": job.get("description", "N/A"),  
        }
        for job in job_list
    ])
    df_ft['city'] = df_ft['location'].str.split(" - ").str[-1]  # Prend la partie après le tiret
    return df_ft


# Transformation pour Indeed
def transform_indeed():
    csv_path = r"C:\Users\lilya\Documents\DataScientest\job_market_project\data\input\indeed-jobs.csv"

    # 1) Lecture du CSV d'origine, qui utilise la virgule comme séparateur
    df_indeed = pd.read_csv(csv_path, sep=",")

    # 2) Création/ajout/restructuration des colonnes demandées

    # a) origin => toujours 'Indeed'
    df_indeed['origin'] = 'Indeed'

    # b) id => incrémental sur 10 digits (zfill(10))
    df_indeed['id'] = range(1, len(df_indeed) + 1)  # 1, 2, 3, ...
    df_indeed['id'] = df_indeed['id'].astype(str).str.zfill(10)  # "0000000001", ...

    # c) title => déjà présent, on garde la colonne existante "title"
    # d) publication_date => date du jour au format "YYYY-MM-DD"
    df_indeed['publication_date'] = datetime.now().strftime('%Y-%m-%d')

    # f) location => inchangé (déjà présent)
    # g) city => garder uniquement les lettres (et espaces) depuis la colonne location
    df_indeed['city'] = df_indeed['location'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', str(x)).strip())

    # h) code_postal => garder uniquement les chiffres depuis la colonne location
    df_indeed['code_postal'] = df_indeed['location'].apply(lambda x: re.sub(r'\D', '', str(x)))

    # i) salary => déjà présent sous ce nom
    # j) contract_type => None
    df_indeed['contract_type'] = None

    # k) category => None
    df_indeed['category'] = None

    # l) description => renommer "summary" en "description"
    df_indeed.rename(columns={'summary': 'description'}, inplace=True)

    # 3) Réordonner les colonnes selon la liste souhaitée
    colonnes_finales = [
        'origin',
        'id',
        'title',
        'publication_date',
        'company',
        'location',
        'city',
        'code_postal',
        'salary',
        'contract_type',
        'category',
        'description'
    ]
    df_indeed = df_indeed[colonnes_finales]
    return df_indeed

# Nettoyage et normalisation des données
def clean_data(df):
    """
    Nettoie et normalise les données d'un DataFrame.
    """
    # Nettoyage des descriptions pour retirer les caractères spéciaux
    df["description"] = df["description"].fillna("").apply(lambda x: re.sub(r"\s+", " ", x).strip())

    # Conversion des dates
    df["publication_date"] = pd.to_datetime(df["publication_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Suppression des doublons
    df = df.drop_duplicates(subset=["id", "origin"])

    return df


# Nettoyage et enrichissement des salaires
def clean_salary(df):
    """Applique des transformations sur les salaires."""
    df['salary'] = df.apply(calculate_salary, axis=1)
    return df


def calculate_salary(row):
    """Calcule le salaire mensuel à partir des formats de salaire variés."""
    text = row["salary"]
    if not isinstance(text, str):
        return None
    if text.startswith("Mensuel"):
        match = re.search(r"Mensuel de (\d+\.?\d*)", text)
        return round(float(match.group(1)), 1) if match else None
    elif text.startswith("Annuel"):
        match = re.search(r"Annuel de (\d+\.?\d*)", text)
        return round(float(match.group(1)) / 12, 1) if match else None
    elif text.startswith("Horaire"):
        match = re.search(r"Horaire de (\d+\.?\d*)", text)
        return round(float(match.group(1)) * 151.67, 1) if match else None
    else:
        return None


# Fusion des données
def merge_data(df_ft, df_adzuna, df_indeed):
    """Fusionne les DataFrames Adzuna et France Travail."""
    return pd.concat([df_ft, df_adzuna, df_indeed], ignore_index=True)


# Exportation des données
def save_to_csv(df, filename):
    """Sauvegarde le DataFrame en CSV dans le répertoire d'entrée."""
    input_dir = r"C:\Users\lilya\Documents\DataScientest\job_market_project\data\input"
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    file_path = os.path.join(input_dir, filename)
    if not df.empty:
        df.to_csv(file_path, sep=';', index=False)
        print(f"Les données ont été sauvegardées dans le fichier : {file_path}")
    else:
        print(f"Aucune donnée à sauvegarder pour {filename}.")

# chercher le code postal
def get_postal_code(city):
    """Retourne le code postal d'une commune en France en utilisant l'API geo.api.gouv.fr.
       Si la ville contient 'Arrondissement' ou est égale à 'PARIS', retourne 75000."""
    # Vérifier si la ville est Paris ou contient 'Arrondissement'
    if city.upper() == "PARIS" or "ARRONDISSEMENT" in city.upper():
        return 75000  # Paris et ses arrondissements ont un code postal 75000
    adjusted_city = city
    if "CORSE" in city.upper():
        adjusted_city = "Ajaccio"  # Remplacer "Corse" par "Ajaccio" ou une autre ville de Corse spécifique
        url = f"https://geo.api.gouv.fr/communes?nom={adjusted_city}&fields=codesPostaux&format=json&geometry=centre"
    else:
        # Sinon, on récupère le code postal habituel
        url = f"https://geo.api.gouv.fr/communes?nom={city}&fields=codesPostaux&format=json&geometry=centre"
        response = requests.get(url)
    
        if response.status_code == 200 and response.json():
            # Retourner le premier code postal trouvé
            return response.json()[0]['codesPostaux'][0]
        else:
            return None  # Aucun résultat trouvé
# Main
if __name__ == "__main__":
    try:
        # Extraction des données France Travail
        print("Extraction des données France Travail...")
        client_id = os.environ.get("ID_CLIENT")
        client_secret = os.environ.get("SECRET_KEY")
        ft_token = post_token(client_id, client_secret)
        france_travail_results = extract_ft_data(ft_token, motsCles="", codePostal="", max_results=10000)
        
        # Extraction des données Adzuna
        print("Extraction des données Adzuna...")
        adzuna_results = extract_adzuna_data(max_pages=100)

        # Transformation des données
        df_adzuna = transform_adzuna(adzuna_results)
        df_ft = transform_france_travail(france_travail_results)

        # Transformation des données Indeed
        print("Extraction des données Indeed...")
        df_indeed = transform_indeed()
        
        
        # Nettoyage des données
        df_adzuna = clean_data(df_adzuna)
        df_ft = clean_data(df_ft)
        df_indeed = clean_data(df_indeed)

        # Calcul des salaires
        df_adzuna = clean_salary(df_adzuna)
        df_ft = clean_salary(df_ft)

        # chercher le code postal
        df_adzuna['code_postal'] = df_adzuna['city'].apply(get_postal_code)

        # Fusionner les données
        df_final = merge_data(df_ft, df_adzuna, df_indeed)

        # Sauvegarder les données transformées
        save_to_csv(df_adzuna, "job_data_adzuna.csv")
        save_to_csv(df_ft, "job_data_ft.csv")
        save_to_csv(df_indeed,"job_data_indeed.csv")

        save_to_csv(df_final, "job_data_combined.csv")
        
        print ("le fichier a bien été cahrgé dans un fichier csv")

    except Exception as e:
        print(f"Une erreur est survenue : {e}")




