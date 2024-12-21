DROP TABLE IF EXISTS dim_qualite_air;
DROP TABLE IF EXISTS dim_validation_transport;
DROP TABLE IF EXISTS dim_population;
DROP TABLE IF EXISTS dim_population_vehicule;

-- Table Qualité de l'air
CREATE TABLE dim_qualite_air (
    id SERIAL PRIMARY KEY,
    nom_site VARCHAR NOT NULL,
    valeur_brute DECIMAL NOT NULL,
    date_de DATE NOT NULL, -- Stocke uniquement la date sans l'heure
    taux_de_saisie DECIMAL NOT NULL,
    couverture_de_donnees DECIMAL NOT NULL
);

-- Table Validation Transport
CREATE TABLE dim_validation_transport (
    id SERIAL PRIMARY KEY,
    station_depart VARCHAR NOT NULL,
    station_arrive VARCHAR NOT NULL,
    nombre_de_validation INTEGER NOT NULL
);

-- Table Population
CREATE TABLE dim_population (
    id SERIAL PRIMARY KEY,
    nom_ville VARCHAR NOT NULL,
    nb_population INTEGER NOT NULL,
    densite_de_pop DECIMAL NOT NULL,
    superficie DECIMAL NOT NULL,
    nombre_de_menage INTEGER NOT NULL
);

-- Table Population Véhiculée
CREATE TABLE dim_population_vehicule (
    id SERIAL PRIMARY KEY,
    nom_ville VARCHAR NOT NULL,
    part_actifs_voiture_travail INTEGER NOT NULL,
    part_actifs_velo_travail INTEGER NOT NULL,
    part_menages_avec_voiture INTEGER NOT NULL
);