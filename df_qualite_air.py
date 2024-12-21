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

try:
    # Chargement des fichiers CSV
    dossier_qa = './DataLake/Qualité air'
    df_qualite_air_argenteuil = pd.read_csv(os.path.join(dossier_qa, 'Export Moy. journalière Argenteuil - NO₂ - 2023-01-01 00_00 - 2024-12-20 10_00.csv'), sep=';')
    df_qualite_air_versailles = pd.read_csv(os.path.join(dossier_qa, 'Export Moy. journalière Versailles - NO₂ - 2023-01-01 00_00 - 2024-12-20 10_00.csv'), sep=';')

    # Combinaison des deux DataFrames
    df_qualite_air = pd.concat([df_qualite_air_argenteuil, df_qualite_air_versailles])

    # Sélection des colonnes nécessaires
    df_qualite_air = df_qualite_air[['nom site', 'valeur brute', 'Date de début', 'taux de saisie', 'couverture de données']]

    # Conversion des types de données
    df_qualite_air['Date de début'] = pd.to_datetime(df_qualite_air['Date de début'], errors='coerce').dt.date

    # Conversion de 'valeur brute' en numérique
    df_qualite_air['valeur brute'] = pd.to_numeric(df_qualite_air['valeur brute'], errors='coerce')

    df_qualite_air['nom site'] = df_qualite_air['nom site'].replace({
        'ARGENTEUIL': 'Argenteuil',
        'VERSAILLES': 'Versailles'
    })


    # Renommer les colonnes
    df_qualite_air.columns = ['nom_site', 'valeur_brute', 'date_debut', 'taux_saisie', 'couverture_donnees']

    # Vérifier les données avant insertion
    print(df_qualite_air.head())

    # Insertion dans la table dim_qualite_air
    df_qualite_air.to_sql('dim_qualite_air', engine, if_exists='replace', index=False)

    # Afficher un message de confirmation
    print("Données insérées avec succès dans la table 'dim_qualite_air'.")
except Exception as e:
    print(f"Erreur : {e}")