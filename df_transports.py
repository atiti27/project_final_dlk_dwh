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

# Dossier contenant les fichiers TXT
dossier = './DataLake/Transports/2023/data-rs-2023'

# Liste pour stocker les DataFrames
dataframes = []

try:
    # Parcourez tous les fichiers dans le dossier
    for filename in os.listdir(dossier):
        if filename.endswith('.txt'):
            filepath = os.path.join(dossier, filename)
            # Lire le fichier txt dans un DataFrame
            df = pd.read_csv(filepath, sep='\t', encoding='latin-1', low_memory=False)
            dataframes.append(df)

    # Concaténer tous les DataFrames
    result = pd.concat(dataframes, ignore_index=True)

    # Filtrer les données par critères (Versailles ou Argenteuil dans LIBELLE_LIGNE)
    df_filtered = result[result['LIBELLE_LIGNE'].str.contains('Versailles|Argenteuil', case=False, na=False)]

    # Sélectionner les colonnes nécessaires
    df_2 = df_filtered[['JOUR', 'LIBELLE_LIGNE', 'NB_VALD']].copy()

    # Regrouper les données par JOUR et LIBELLE_LIGNE, puis calculer la somme des validations
    df_3 = df_2.groupby(['JOUR', 'LIBELLE_LIGNE'])['NB_VALD'].sum().reset_index()

    # Créer une colonne station_depart et station_arrive
    df_3['station_depart'] = df_3['LIBELLE_LIGNE'].str.split(' - ').str[0]
    df_3['station_arrive'] = df_3['LIBELLE_LIGNE'].str.split(' - ').str[1]

    # Renommer les colonnes pour correspondre aux noms de la table PostgreSQL
    df_3.rename(columns={
        'JOUR': 'jour',
        'NB_VALD': 'nombre_de_validation'
    }, inplace=True)

    df_3 = df_3.drop(columns=['LIBELLE_LIGNE'])
    
    # Conversion des types de données si nécessaire
    df_3['jour'] = pd.to_datetime(df_3['jour'], errors='coerce').dt.date
    df_3['nombre_de_validation'] = pd.to_numeric(df_3['nombre_de_validation'], errors='coerce', downcast='integer')

    # Vérifier les données avant insertion
    print(df_3.head())

    # Insérer les données dans PostgreSQL
    df_3.to_sql('dim_validation_transport', engine, if_exists='replace', index=False)
    print("Données insérées avec succès dans la table dim_validation_transport.")
except Exception as e:
    print(f"Erreur : {e}")
