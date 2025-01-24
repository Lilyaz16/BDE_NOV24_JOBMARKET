import pandas as pd
import streamlit as st
import plotly.express as px
import pickle
from snowflake.connector import connect
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from streamlit_lottie import st_lottie
import json
import base64

def add_bg_from_local(image_file):
    with open(image_file, "rb") as f:
        encoded_image = base64.b64encode(f.read()).decode()
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url(data:image/png;base64,{encoded_image});
            background-size: cover;
            background-position: center;
            background-repeat: no-repeat;
            background-attachment: fixed;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

add_bg_from_local('image_fond_4.jpg')





# Connexion à Snowflake en utilisant @st.cache_resource
@st.cache_resource
def init_connection():
    # Utilisation des informations de connexion depuis secrets.toml
    return connect(**st.secrets["snowflake"])

# Charger les données depuis Snowflake en utilisant @st.cache_data
@st.cache_data
def load_data(query):
    conn = init_connection()
    return pd.read_sql(query, conn)

# Charger les données
query = "SELECT CODE_POSTAL FROM job_posting WHERE CODE_POSTAL IS NOT NULL"
df = load_data(query)

# Convertir CODE_POSTAL en chaîne de caractères et extraire le code département
df['CODE_POSTAL'] = df['CODE_POSTAL'].astype(str)  # Convertir en chaîne de caractères
df['code_departement'] = df['CODE_POSTAL'].str[:2]  # Extraire les deux premiers caractères

# Afficher les données dans Streamlit
#st.write("Données avec code département extrait :")
#st.dataframe(df)

# Filtrage des départements
departements = ['92', '75', '91', '94', '93']  # Liste des départements d'intérêt
df_filtered = df[df['code_departement'].isin(departements)]

# Nombre d'offres par département
df_count = df_filtered.groupby('code_departement').size().reset_index(name='count')

# Pages pour la navigation dans Streamlit
pages = ["Contexte du projet", "Exploration des données", "Analyse de données", "Modélisation"]
page = st.sidebar.radio("Aller vers la page :", pages)

if page == pages[0]: 
    st.write("### Contexte du projet")
    st.write("Ce projet a pour but de mettre en avant vos compétences de Data Engineer. Vous allez regrouper des informations sur les offres d’emplois et les compagnies qui les proposent.")
    st.write("Nous commencerons par analyser les offres d'emploi publiées par France Travail.")
    
    st.image("streamlit_image.jpg")
    

elif page == pages[1]:
    st.markdown("""
    <div style="display: flex; justify-content: space-around; margin-bottom: 20px;">
        <div style="background-color: #6c63ff; color: white; padding: 20px; border-radius: 10px; text-align: center;">
            <h3>Nombre Total d'Offres</h3>
            <p style="font-size: 24px; font-weight: bold;">5,000</p>
        </div>
        <div style="background-color: #ff5722; color: white; padding: 20px; border-radius: 10px; text-align: center;">
            <h3>Villes Couvertes</h3>
            <p style="font-size: 24px; font-weight: bold;">250</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
      
    
    
    st.write("### Exploration des données")
    st.dataframe(df.head())
    st.write("Dimensions du dataframe :")
    st.write(df.shape)
    if st.checkbox("Afficher les valeurs manquantes"): 
        st.dataframe(df.isna().sum())
    if st.checkbox("Afficher les doublons"): 
        st.write(df.duplicated().sum())

elif page == pages[2]:
    st.write("### Analyse de données")
    
    # Visualisation avec Plotly pour le nombre d'offres par département
    fig2 = px.bar(df_count, 
                  x="code_departement", 
                  y="count", 
                  title="Nombre d'offres d'emploi par département",
                  labels={'code_departement': 'Département', 'count': 'Nombre d\'offres'},
                  color="count", 
                  color_continuous_scale='Viridis')
    st.plotly_chart(fig2)
    # Utilisation d'une checkbox pour sélectionner un département
    if st.checkbox("Sélectionnez les départements à afficher"):
        selected_depts = st.multiselect(
            "Choisissez les départements",
            options=["75", "91", "92", "93", "94"],
            default=["75", "91", "92", "93", "94"]
        )
    
        # Filtrer le dataframe en fonction des départements sélectionnés
        df_filtered = df[df['code_departement'].isin(selected_depts)]
    
    else:
        st.write("Veuillez sélectionner un ou plusieurs départements.")
    # Charger les données depuis la base Snowflake pour la catégorie
    query = "SELECT CATEGORY FROM job_posting"
    df = load_data(query)
    
    # Vérifier si la colonne 'CATEGORY' existe
    if 'CATEGORY' in df.columns:
        # Remplacer "Unknown" par "Non spécifié"
        df['CATEGORY'] = df['CATEGORY'].replace("Unknown", "Non spécifié")
    
        # Calculer le nombre d'offres par catégorie
        category_count = df['CATEGORY'].value_counts().reset_index()
        category_count.columns = ['CATEGORY', 'nombre_offres']
    
        # Filtrer les catégories où le nombre d'offres est inférieur à 3
        category_count = category_count[category_count['nombre_offres'] >= 40]
    
        if not category_count.empty:
            # Créer un graphique de barres montrant le nombre d'offres par catégorie
            fig_category = px.bar(category_count, 
                              x="CATEGORY", 
                              y="nombre_offres", 
                              title="Nombre d'offres d'emploi par catégorie avec plus de 40 offres",
                              labels={'CATEGORY': 'Catégorie', 'nombre_offres': 'Nombre d\'offres'},
                              color="nombre_offres", 
                              color_continuous_scale='Viridis')
        
            # Afficher le graphique dans Streamlit
            st.plotly_chart(fig_category)
        else:
            st.write("Aucune catégorie avec plus de 40 offres .")
        
    else:
        st.error("La colonne 'CATEGORY' est absente dans les données.")

    query = "SELECT COMPANY FROM job_posting"
    df = load_data(query)
    if 'COMPANY' in df.columns:
        # Calculer le nombre d'offres par entreprise
        company_count = df['COMPANY'].value_counts().reset_index()
        company_count.columns = ['COMPANY', 'nombre_offres']
    
        # Filtrer pour obtenir le top 5 des entreprises
        top_5_companies = company_count.head(5)
    
        if not top_5_companies.empty:
            # Créer un graphique à barres pour le top 5 des entreprises qui recrutent
            fig_company = px.bar(top_5_companies, 
                             x="COMPANY", 
                             y="nombre_offres", 
                             title="Top 5 des entreprises qui recrutent",
                             labels={'COMPANY': 'Entreprise', 'nombre_offres': 'Nombre d\'offres'},
                             color="nombre_offres", 
                             color_continuous_scale='Blues')
        
            # Afficher le graphique dans Streamlit
            st.plotly_chart(fig_company)
        else:
            st.write("Aucune entreprise n'a d'offres d'emploi disponibles.")
        
    else:
        st.error("La colonne 'company' est absente dans les données.")
    
    query = "SELECT CITY, SALARY FROM job_posting"
    df = load_data(query)
    if 'CITY' in df.columns and 'SALARY' in df.columns:
        # Convertir la colonne 'salary' en numérique si ce n'est pas déjà le cas
        df['SALARY'] = pd.to_numeric(df['SALARY'], errors='coerce')
    
        # Calculer les salaires moyens par ville
        city_salary_avg = df.groupby('CITY')['SALARY'].mean().reset_index()
    
        # Trier les villes par salaire moyen (du plus élevé au plus bas)
        city_salary_avg = city_salary_avg.sort_values(by='SALARY', ascending=False)
    
        # Sélectionner uniquement le Top 10 des villes
        top_10_city_salary = city_salary_avg.head(10)
    
        # Créer un graphique en courbe (ligne) montrant la fluctuation des salaires selon les villes
        fig_salary = px.line(top_10_city_salary, 
                         x='CITY', 
                         y='SALARY', 
                         title="Top 10 des salaires moyens selon les villes",
                         labels={'city': 'Ville', 'salary': 'Salaire moyen'},
                         markers=True)
    
        # Afficher le graphique dans Streamlit
        st.plotly_chart(fig_salary)
    
    else:
        st.error("Les colonnes 'city' ou 'salary' sont absentes dans les données.")

    query = "SELECT CODE_POSTAL FROM job_posting WHERE CODE_POSTAL IS NOT NULL"
    df = load_data(query)

    # Convertir CODE_POSTAL en chaîne de caractères et extraire le code département
    df['CODE_POSTAL'] = df['CODE_POSTAL'].astype(str)  # Convertir en chaîne de caractères
    df['code_departement'] = df['CODE_POSTAL'].str[:2]  # Extraire les deux premiers caractères

    # Afficher les données dans Streamlit
    st.write("Données avec code département extrait :")
    st.dataframe(df)
    
    # Compter le nombre d'offres par code_departement
    city_offer_count = df['code_departement'].value_counts().reset_index()
    city_offer_count.columns = ['code_departement', 'nombre_offres']
    
    # Créer un graphique pie (camembert) pour le nombre d'offres par ville
    fig_pie = px.pie(city_offer_count, 
                    names='code_departement', 
                    values='nombre_offres', 
                    title="Nombre d'offres par ville",
                    labels={'code_departement': 'Département', 'nombre_offres': 'Nombre d\'offres'})
    
    # Afficher le graphique dans Streamlit
    st.plotly_chart(fig_pie)
    
    

elif page == pages[3]:
    st.write("### Modélisation")
    st.write("Dans cette section, nous procéderons à l'analyse plus avancée et aux modèles prédictifs sur les données.")
    def recommend_jobs(user_input, model_path, top_n=5):
    # Charger le modèle
        with open(model_path, 'rb') as f:
            vectorizer, tfidf_matrix, job_data = pickle.load(f)
        
        # Transformer la requête utilisateur en vecteur TF-IDF
        user_vector = vectorizer.transform([user_input])
        
        # Calculer les similarités entre la requête et les offres d'emploi
        similarities = cosine_similarity(user_vector, tfidf_matrix)
        
        # Identifier les N documents les plus similaires
        top_indices = similarities.argsort()[0, -top_n:][::-1]  # Indices des offres les plus proches
        
        # Extraire les lignes correspondantes
        recommendations = job_data.iloc[top_indices]
        return recommendations[['TITLE', 'LOCATION', 'COMPANY', 'DESCRIPTION']]
    
    # interface utilisateur
    st.write("### Modélisation : Recommandation d'offres d'emploi")
    
    st.write("Entrez vos préférences pour obtenir les offres d'emploi les plus pertinentes :")
    
    # Champ de saisie pour l'utilisateur
    user_query = st.text_input("Recherche (par exemple, 'data engineer à Paris')", "")
    
    # Chemin du fichier modèle
    model_file = r"C:\Users\lilya\Documents\DataScientest\job_market_project\ml\models\recommendation_model.pkl"
    
    # Bouton pour générer des recommandations
    if st.button("Rechercher"):
        if user_query.strip():
            try:
                # Obtenir les recommandations
                recommended_jobs = recommend_jobs(user_query, model_file, top_n=5)
                
                # Afficher les résultats
                st.write("### Recommandations :")
                st.dataframe(recommended_jobs)
            except Exception as e:
                st.error(f"Une erreur s'est produite : {e}")
        else:
            st.warning("Veuillez entrer une recherche avant de cliquer sur Rechercher.")