import pickle
from sklearn.metrics.pairwise import cosine_similarity

def recommend_jobs(user_input, model_path, top_n=5):
    # Charger le modèle
    with open(model_path, 'rb') as f:
        vectorizer, similarity_matrix, job_data = pickle.load(f)
    
    # Transformer les préférences utilisateur en vecteur TF-IDF
    user_vector = vectorizer.transform([user_input])
    
    # Vérifiez les dimensions avant de calculer les similarités
    if user_vector.shape[1] != similarity_matrix.shape[1]:
        print(f"Taille du vocabulaire du vectorizer : {len(vectorizer.vocabulary_)}")
        print(f"Dimensions de similarity_matrix : {similarity_matrix.shape}")
        print(f"Dimensions du vecteur utilisateur : {user_vector.shape}")
        raise ValueError(
            f"Incompatibilité entre le vocabulaire du vectorizer et la matrice de similarité. "
            f"Assurez-vous que le fichier modèle est cohérent."
        )

    # Calculer les similarités entre ce vecteur et toutes les offres
    similarities = cosine_similarity(user_vector, similarity_matrix)
    
    # Identifier les indices des offres les plus similaires
    recommended_indices = similarities.argsort()[0, -top_n:][::-1]
    recommendations = job_data.iloc[recommended_indices]
    
    # Retourner uniquement les colonnes pertinentes
    return recommendations[['TITLE', 'LOCATION', 'COMPANY', 'DESCRIPTION']]





if __name__ == "__main__":
    user_query = "data engineer à Paris et salary"
    model_file = r"C:\Users\lilya\Documents\DataScientest\job_market_project\ml\models\recommendation_model.pkl"
    
    recommended_jobs = recommend_jobs(user_query, model_file, top_n=5)
    print(recommended_jobs)
