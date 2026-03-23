# 🏦 Dashboard Banques Sénégal

Tableau de bord interactif d'analyse et de positionnement des banques au Sénégal,
basé sur les données officielles de la **BCEAO (2015–2022)**.

Développé avec **Python · Dash · MongoDB · Plotly**.

---

## 📋 Fonctionnalités

- Analyse comparative du bilan (actif, emplois, ressources, fonds propres)
- Analyse financière (PNB, résultat net, coefficient d'exploitation)
- Ratios financiers (ROA, ROE, solvabilité, marge nette)
- Évolution temporelle par banque et par groupe
- Carte géographique des banques à Dakar
- Comparaison multidimensionnelle (radar)
- Export PDF par banque et par année
- Filtres dynamiques (année, groupe bancaire, sélection de banques)

---

## 🗂️ Structure du projet

```
banques-senegal/
│
├── dashboard/
│   └── app.py                      # Application Dash principale
│
├── scripts/
│   ├── insertion_pdf_2020_2022.py  # Extraction PDF BCEAO → MongoDB
│   ├── nettoyage.py                # Nettoyage et normalisation des données
│   └── fusion_data.py             # Calcul des ratios → indicateurs_propre
│
├── tests/
│   └── test_data.py                # Tests unitaires (intégrité des données)
│
├── data/
│   └── pdf_bceao/                  # PDFs BCEAO (non versionnés)
│
├── .env.example                    # Modèle de configuration (sans secrets)
├── .gitignore                      # Fichiers exclus du versionnage
├── Procfile                        # Commande de démarrage pour Render/Heroku
├── requirements.txt                # Dépendances Python
└── README.md                       # Ce fichier
```

---

## ⚙️ Installation locale

### Prérequis

- Python 3.10+
- MongoDB installé localement **ou** un compte MongoDB Atlas
- Git

### 1. Cloner le dépôt

```bash
git clone https://github.com/FATOUMBODJ11/banques_senegal.git
cd banques_senegal
```

### 2. Créer un environnement virtuel

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Mac / Linux
source .venv/bin/activate
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Configurer les variables d'environnement

```bash
cp .env.example .env
```

Ouvrez `.env` et renseignez vos valeurs :

```
MONGO_URI=mongodb://localhost:27017
MONGO_DB=banques_senegal
```

---

## 🚀 Lancer l'application

### Ordre d'exécution obligatoire

```bash
# Étape 1 — Extraire les données du PDF BCEAO (2020–2022)
python scripts/insertion_pdf_2020_2022.py

# Étape 2 — Nettoyer et normaliser les données
python scripts/nettoyage.py

# Étape 3 — Fusionner et calculer les ratios financiers
python scripts/fusion_data.py

# Étape 4 — Lancer le dashboard
python dashboard/app.py
```

Ouvrez votre navigateur sur : **http://127.0.0.1:8050**

---

## 📊 Sources de données

| Source | Période | Format | Description |
|--------|---------|--------|-------------|
| BCEAO — Rapport annuel | 2015–2020 | Excel | Bilans et comptes de résultat |
| BCEAO — Rapport annuel | 2020–2022 | PDF | Extraction automatique via `pdfplumber` |

### Extraction PDF

Le script `insertion_pdf_2020_2022.py` :
1. Ouvre le PDF BCEAO avec `pdfplumber`
2. Navigue vers les pages de bilan et de résultat de chaque banque
3. Extrait les valeurs numériques pour 3 années (2020, 2021, 2022)
4. Insère directement dans MongoDB (collections `indicateurs` et `indicateurs_propre`)

### Pipeline de nettoyage

```
PDF / Excel → MongoDB brut
     ↓
nettoyage.py     (types, groupes, valeurs manquantes, outliers)
     ↓
fusion_data.py   (ratios financiers, tri, indicateurs_propre)
     ↓
dashboard/app.py (visualisation)
```

---

## 🧪 Tests unitaires

```bash
python -m pytest tests/ -v
```

Les tests vérifient :
- La collection MongoDB n'est pas vide
- Les colonnes obligatoires sont présentes
- Les bilans sont positifs
- Les années sont dans la plage 2010–2025
- Les groupes bancaires sont connus
- Les ratios financiers sont dans des plages cohérentes
- Au moins 20 banques sont présentes

---

## 🌐 Déploiement (Render + MongoDB Atlas)

Voir la section [Déploiement](#) du wiki ou suivre ces étapes :

1. Créer un cluster gratuit sur [MongoDB Atlas](https://cloud.mongodb.com)
2. Importer les données avec `mongodump` / `mongorestore`
3. Connecter le dépôt GitHub sur [Render](https://render.com)
4. Configurer les variables d'environnement `MONGO_URI` et `MONGO_DB`
5. Déployer — l'URL publique est générée automatiquement

---

## 🛠️ Technologies utilisées

| Technologie | Usage |
|-------------|-------|
| Python 3.11 | Langage principal |
| Dash + Flask | Framework web interactif |
| Plotly | Visualisations graphiques |
| Dash Bootstrap Components | Interface utilisateur |
| MongoDB + PyMongo | Base de données NoSQL |
| pdfplumber | Extraction de données PDF |
| ReportLab | Génération de rapports PDF |
| pandas + numpy | Traitement des données |
| pytest | Tests unitaires |
| Render | Hébergement cloud |

---

## 👩‍💻 Auteur

**Fatoumbodj** — Projet académique BCEAO  
GitHub : [@FATOUMBODJ11](https://github.com/FATOUMBODJ11)