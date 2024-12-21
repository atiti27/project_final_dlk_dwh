import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# Configuration de la base de données
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Création de l'engine SQLAlchemy
db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# Test de la connexion à la base de données
try:
    with engine.connect() as connection:
        print("Connexion réussie à la base de données !")
except Exception as e:
    print(f"Erreur de connexion: {e}")

dossier = './DataLake/Données démographiques/pop_vehicule'

try:
    # Charger le fichier CSV
    filepath = os.path.join(dossier, 'population_vehicule.csv')
    df = pd.read_csv(filepath, sep=';', header=2)

    # Vérifier les colonnes
    print("Colonnes avant nettoyage :", df.columns)

    # Nettoyer les noms de colonnes
    df.columns = df.columns.str.strip()
    print("Colonnes après nettoyage :", df.columns)

    # Filtrer les colonnes nécessaires
    df_filtered = df[[
        'Libellé',
        'Part des actifs occ 15 ans ou plus voiture pour travailler 2021',
        'Part des ménages ayant au moins 1 voiture 2021'
    ]].copy()

    # Renommer les colonnes pour correspondre à la table PostgreSQL
    df_filtered.rename(columns={
        'Libellé': 'nom_ville',
        'Part des actifs occ 15 ans ou plus voiture pour travailler 2021': 'part_actifs_voiture_travail',
        'Part des ménages ayant au moins 1 voiture 2021': 'part_menages_avec_voiture'
    }, inplace=True)

    # Filtrer pour obtenir uniquement les lignes où 'nom_ville' est exactement 'Versailles' ou 'Argenteuil'
    df_filtered = df_filtered[df_filtered['nom_ville'].isin(['Versailles', 'Argenteuil'])]

    # Conversion des types de données si nécessaire
    df_filtered['part_actifs_voiture_travail'] = pd.to_numeric(df_filtered['part_actifs_voiture_travail'], errors='coerce')
    df_filtered['part_menages_avec_voiture'] = pd.to_numeric(df_filtered['part_menages_avec_voiture'], errors='coerce')

    # Vérifier les données avant insertion
    print(df_filtered.head())

    # Insérer les données dans PostgreSQL
    df_filtered.to_sql('dim_population_vehicule', engine, if_exists='replace', index=False)
    print("Données insérées avec succès dans la table dim_population_vehicule.")
except Exception as e:
    print(f"Erreur : {e}")
