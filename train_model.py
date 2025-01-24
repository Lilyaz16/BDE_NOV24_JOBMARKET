import snowflake.connector
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pickle
from stop_words import get_stop_words
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

# Connexion à Snowflake
def connect_to_snowflake():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

# Fonction pour récupérer les données depuis Snowflake
def fetch_data_from_snowflake(query):
    conn = connect_to_snowflake()
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

# Fonction pour entraîner le modèle
def train_model(sql_query, model_output_path):
    # Récupérer les données depuis Snowflake
    job_data = fetch_data_from_snowflake(sql_query)
    
    # Vérifiez que les colonnes nécessaires existent
    required_columns = ['TITLE', 'DESCRIPTION', 'LOCATION', 'COMPANY']
    for col in required_columns:
        if col not in job_data.columns:
            raise KeyError(f"La colonne requise '{col}' est absente des données récupérées.")
    
    # Combiner les colonnes pertinentes pour la description
    job_data['combined_features'] = (
        job_data['TITLE'] + " " + 
        job_data['DESCRIPTION'] + " " + 
        job_data['LOCATION'] + " " + 
        job_data['COMPANY']
    )
    job_data['combined_features'] = job_data['combined_features'].fillna("")
    
    # TF-IDF pour vectoriser les descriptions d'offres d'emploi
    french_stopwords = get_stop_words('fr')
    vectorizer = TfidfVectorizer(stop_words=french_stopwords)
    tfidf_matrix = vectorizer.fit_transform(job_data['combined_features'])
    
    # Enregistrer le modèle
    with open(model_output_path, 'wb') as f:
        pickle.dump((vectorizer, tfidf_matrix, job_data), f)
    
    print(f"Modèle entraîné et enregistré dans {model_output_path}")

# Main
if __name__ == "__main__":
    sql_query = """
    SELECT 
        TITLE, 
        DESCRIPTION, 
        LOCATION,
        COMPANY 
    FROM JOB_POSTING
    """  # Adaptez les noms des colonnes si nécessaire

    model_output_path = r"C:\Users\lilya\Documents\DataScientest\job_market_project\ml\models\recommendation_model.pkl"
    
    train_model(sql_query, model_output_path)






