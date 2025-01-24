import snowflake.connector
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Connexion à Snowflake
def connect_to_snowflake():
    connection = snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    return connection



# Fichier CSV local à charger
CSV_PATH = r"C:\Users\lilya\Documents\DataScientest\job_market_project\data\input\job_data_combined.csv"
# Nom du stage, table, et objet final
my_stage = "MY_STAGE"             
job_posting = "JOB_POSTING"



try:
    # On crée un curseur pour exécuter les commandes SQL
    conn = connect_to_snowflake()
    cur = conn.cursor()

    # 1) Créer (ou remplacer) un stage
    create_stage_sql = f"""
    CREATE OR REPLACE STAGE {my_stage}
    """
    cur.execute(create_stage_sql)
    print(f"Stage {my_stage} créé.")

    # 2) Créer (ou remplacer) la table 
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {job_posting} (
      origin VARCHAR,
      id VARCHAR,
      title VARCHAR,
      publication_date DATE,
      company VARCHAR,
      location VARCHAR,
      city VARCHAR,
      code_postal VARCHAR,
      salary VARCHAR,
      contract_type VARCHAR,
      category VARCHAR,
      description VARCHAR,
      INSERTED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP,
      UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP
    )
    """
    cur.execute(create_table_sql)
    print(f"Table {job_posting} créée.")

    # 3) Envoyer le fichier local dans le stage (PUT)
    # Notez que le connecteur Python Snowflake ne prend pas en charge directement la commande PUT
    # On peut contourner ce problème en appelant la commande PUT via le cursor.execute 
    # mais seulement si vous utilisez SnowSQL en backend.  
    # Sinon, on peut utiliser la méthode "upload_stream" du connecteur (API v2).
    # Pour simplifier, on va exécuter la commande PUT comme du SQL.
    
    put_sql = f"""
    PUT file://{CSV_PATH}
    @{my_stage}
    AUTO_COMPRESS=FALSE
    OVERWRITE=TRUE
    """
    cur.execute(put_sql)
    print(f"Fichier {CSV_PATH} chargé dans le stage {my_stage}.")

    # 4) Copie les données vers la table
    copy_sql = f"""
    COPY INTO JOB_POSTING
    FROM @MY_STAGE/job_data_combined.csv
    FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ';'
    ENCODING='UTF8'
    PARSE_HEADER=TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
   
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    ESCAPE_UNENCLOSED_FIELD = NONE
    )
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """
    cur.execute(copy_sql)
    nb_rows = cur.fetchone()[0]  # Renvoie le nombre de lignes chargées
    print(f"{nb_rows} lignes importées dans {job_posting}.")

    # 5) Vérifier le nombre total de lignes dans la table
    cur.execute(f"SELECT COUNT(*) FROM {job_posting}")
    total_rows = cur.fetchone()[0]
    print(f"Total de {total_rows} lignes dans {job_posting}.")

finally:
    cur.close()
    conn.close()

