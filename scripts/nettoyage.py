"""
nettoyage.py
Nettoyage et normalisation des données depuis MongoDB.
Ce script :
  1. Lit la collection 'indicateurs_propre' depuis MongoDB
  2. Harmonise les noms de colonnes
  3. Normalise les groupes bancaires
  4. Corrige les types
  5. Sauvegarde la collection nettoyée dans MongoDB

Prérequis : avoir lancé insertion_pdf_2020_2022.py pour avoir les données 2020-2022
"""

import os
import numpy as np
import pandas as pd
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "banques_senegal")

# ── Colonnes numériques attendues ─────────────────────────────────────────────
COLS_NUM = [
    "emploi", "bilan", "ressources", "fonds_propre",
    "effectif", "agence", "compte",
    "interets_produits", "interets_charges",
    "commissions_produits", "commissions_charges",
    "produit_net_bancaire", "charges_generales_exploitation",
    "dotations_amortissements", "resultat_brut_exploitation",
    "cout_du_risque", "resultat_exploitation",
    "resultat_avant_impot", "impots_benefices", "resultat_net",
]

# ── Mapping renommage colonnes (noms bruts MongoDB → noms normalisés) ─────────
RENOMMAGE = {
    "Sigle":                    "sigle",
    "Goupe_Bancaire":           "groupe_bancaire",
    "ANNEE":                    "annee",
    "EMPLOI":                   "emploi",
    "BILAN":                    "bilan",
    "RESSOURCES":               "ressources",
    "FONDS.PROPRE":             "fonds_propre",
    "EFFECTIF":                 "effectif",
    "AGENCE":                   "agence",
    "COMPTE":                   "compte",
    "RESULTAT.NET":             "resultat_net",
    "PRODUIT.NET.BANCAIRE":     "produit_net_bancaire",
    "CHARGES.GENERALES.D'EXPLOITATION":             "charges_generales_exploitation",
    "RESULTAT.BRUT.D'EXPLOITATION":                 "resultat_brut_exploitation",
    "COÛT.DU.RISQUE":           "cout_du_risque",
    "RESULTAT.D'EXPLOITATION":  "resultat_exploitation",
    "RESULTAT.AVANT.IMPÔT":     "resultat_avant_impot",
    "IMPÔTS.SUR.LES.BENEFICES": "impots_benefices",
    "INTERETS.ET.PRODUITS.ASSIMILES":   "interets_produits",
    "NTERETS.ET.CHARGES.ASSIMILEES":    "interets_charges",
    "COMMISSIONS.(PRODUITS)":   "commissions_produits",
    "COMMISSIONS.(CHARGES)":    "commissions_charges",
    "DOTATIONS.AUX.AMORTISSEMENTS.ET.AUX.DEPRECIATIONS.DES.IMMOBILISATIONS.INCORPORELLES.ET.CORPORELLES": "dotations_amortissements",
}

# ── Groupes bancaires connus ──────────────────────────────────────────────────
GROUPES_CONNUS = {
    "BAS":  "Groupes Continentaux",   "BCIM": "Groupes Internationaux",
    "BDK":  "Groupes Règionaux",      "BGFI": "Groupes Continentaux",
    "BHS":  "Groupes Locaux",         "BICIS":"Groupes Internationaux",
    "BIS":  "Groupes Règionaux",      "BNDE": "Groupes Locaux",
    "BOA":  "Groupes Règionaux",      "BRM":  "Groupes Locaux",
    "BSIC": "Groupes Règionaux",      "CBAO": "Groupes Internationaux",
    "CBI":  "Groupes Locaux",         "CDS":  "Groupes Locaux",
    "CISA": "Groupes Locaux",         "CITIBANK": "Groupes Internationaux",
    "ECOBANK": "Groupes Règionaux",   "FBNBANK":  "Groupes Internationaux",
    "LBA":  "Groupes Règionaux",      "LBO":  "Groupes Locaux",
    "NSIA Banque": "Groupes Règionaux","ORABANK": "Groupes Règionaux",
    "SGBS": "Groupes Internationaux", "UBA":  "Groupes Internationaux",
}

MAPPING_GROUPES = {
    "continentaux":   "Groupes Continentaux",
    "regionaux":      "Groupes Règionaux",
    "regional":       "Groupes Règionaux",
    "international":  "Groupes Internationaux",
    "internationaux": "Groupes Internationaux",
    "local":          "Groupes Locaux",
    "locaux":         "Groupes Locaux",
}


# ── 1. Chargement depuis MongoDB ──────────────────────────────────────────────

def charger_depuis_mongo() -> pd.DataFrame:
    """Charge et fusionne indicateurs (Excel) + indicateurs_propre (PDF)."""
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    # Source 1 : données Excel 2015-2020
    docs_excel = list(db["indicateurs"].find({}, {"_id": 0}))
    df_excel   = pd.DataFrame(docs_excel)

    # Source 2 : données déjà insérées (pdf ou propre)
    docs_propre = list(db["indicateurs_propre"].find({}, {"_id": 0}))
    df_propre   = pd.DataFrame(docs_propre)

    client.close()

    print(f"  📥  Collection 'indicateurs'        : {len(df_excel)} documents")
    print(f"  📥  Collection 'indicateurs_propre' : {len(df_propre)} documents")

    # Renommer les colonnes brutes si nécessaire
    df_excel  = df_excel.rename(columns=RENOMMAGE)
    df_propre = df_propre.rename(columns=RENOMMAGE)

    # Fusionner et dédupliquer (sigle + annee)
    df = pd.concat([df_excel, df_propre], ignore_index=True)
    avant = len(df)
    df = df.drop_duplicates(subset=["sigle", "annee"], keep="last")
    apres = len(df)
    if avant != apres:
        print(f"  🗑️   {avant - apres} doublon(s) supprimé(s)")

    annees = sorted(df["annee"].dropna().unique())
    print(f"  ✅  Dataset fusionné : {len(df)} lignes — années {annees}")
    return df


# ── 2. Nettoyage ──────────────────────────────────────────────────────────────

def corriger_types(df: pd.DataFrame) -> pd.DataFrame:
    """Force les types corrects sur toutes les colonnes."""
    for col in COLS_NUM:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")
    df["sigle"] = df["sigle"].astype(str).str.strip().str.upper()
    # Correction casse NSIA
    df.loc[df["sigle"] == "NSIA BANQUE", "sigle"] = "NSIA Banque"
    return df


def normaliser_groupes(df: pd.DataFrame) -> pd.DataFrame:
    """Assure que groupe_bancaire est renseigné et normalisé."""
    if "groupe_bancaire" not in df.columns:
        df["groupe_bancaire"] = None

    mask = df["groupe_bancaire"].isna() | (df["groupe_bancaire"] == "")
    df.loc[mask, "groupe_bancaire"] = df.loc[mask, "sigle"].map(GROUPES_CONNUS)

    def normaliser(val):
        if not isinstance(val, str):
            return val
        v = val.lower()
        for cle, norm in MAPPING_GROUPES.items():
            if cle in v:
                return norm
        return val

    df["groupe_bancaire"] = df["groupe_bancaire"].apply(normaliser)
    return df


def traiter_valeurs_manquantes(df: pd.DataFrame) -> pd.DataFrame:
    """Imputation médiane par banque pour les colonnes critiques."""
    cols_critiques = ["bilan", "emploi", "ressources",
                      "produit_net_bancaire", "resultat_net"]
    rapport = []
    for col in COLS_NUM:
        if col not in df.columns:
            continue
        nb = df[col].isna().sum()
        if nb > 0:
            rapport.append(f"    {col:<45} {nb:>3} manquants ({nb/len(df)*100:.0f}%)")
        if col in cols_critiques and nb > 0:
            df[col] = df.groupby("sigle")[col].transform(
                lambda x: x.fillna(x.median())
            )
    if rapport:
        print("  📊  Valeurs manquantes :")
        for r in rapport:
            print(r)
    return df


def detecter_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remplace les outliers extrêmes (IQR × 3) par la médiane."""
    cols_check = ["bilan", "emploi", "ressources", "fonds_propre",
                  "produit_net_bancaire", "resultat_net"]
    total = 0
    for col in cols_check:
        if col not in df.columns:
            continue
        Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
        IQR = Q3 - Q1
        mask = (df[col] < Q1 - 3*IQR) | (df[col] > Q3 + 3*IQR)
        n = mask.sum()
        if n > 0:
            med = df[col].median()
            df.loc[mask, col] = med
            total += n
            print(f"    {col} : {n} outlier(s) → médiane ({med:,.0f})")
    if total == 0:
        print("    Aucun outlier extrême détecté")
    return df


def trier_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Trie et ordonne les colonnes."""
    df = df.sort_values(["sigle", "annee"]).reset_index(drop=True)
    cols_ordre = [
        "sigle", "groupe_bancaire", "annee",
        "bilan", "emploi", "ressources", "fonds_propre",
        "effectif", "agence", "compte",
        "interets_produits", "interets_charges",
        "commissions_produits", "commissions_charges",
        "produit_net_bancaire", "charges_generales_exploitation",
        "dotations_amortissements", "resultat_brut_exploitation",
        "cout_du_risque", "resultat_exploitation",
        "resultat_avant_impot", "impots_benefices", "resultat_net",
        "source",
    ]
    presentes = [c for c in cols_ordre if c in df.columns]
    autres    = [c for c in df.columns if c not in presentes]
    return df[presentes + autres]


# ── 3. Sauvegarde MongoDB ─────────────────────────────────────────────────────

def sauvegarder_mongo(df: pd.DataFrame):
    """Sauvegarde le dataset nettoyé dans 'indicateurs_nettoye'."""
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    df_clean = df.where(pd.notnull(df), None)
    docs     = df_clean.to_dict(orient="records")

    col = db["indicateurs_nettoye"]
    col.drop()
    col.insert_many(docs)
    col.create_index([("sigle", ASCENDING), ("annee", ASCENDING)])

    print(f"\n  ✅  Collection 'indicateurs_nettoye' : {col.count_documents({})} documents")
    client.close()


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  NETTOYAGE — Données MongoDB")
    print("=" * 60)

    print("\n📂  Chargement depuis MongoDB...")
    df = charger_depuis_mongo()

    print("\n🔧  Nettoyage...")

    print("  [1/4] Correction des types...")
    df = corriger_types(df)

    print("  [2/4] Normalisation des groupes bancaires...")
    df = normaliser_groupes(df)

    print("  [3/4] Traitement des valeurs manquantes...")
    df = traiter_valeurs_manquantes(df)

    print("  [4/4] Détection des outliers...")
    df = detecter_outliers(df)

    df = trier_dataset(df)

    print("\n🗄️   Sauvegarde dans MongoDB...")
    sauvegarder_mongo(df)

    print("\n" + "=" * 60)
    print("  BILAN")
    print("=" * 60)
    print(f"  Lignes      : {len(df)}")
    print(f"  Banques     : {df['sigle'].nunique()} — {sorted(df['sigle'].unique())}")
    print(f"  Années      : {sorted(df['annee'].dropna().unique())}")
    print(f"  Colonnes    : {len(df.columns)}")
    print("\n🎉  Nettoyage terminé — lancez fusion_data.py")


if __name__ == "__main__":
    main()