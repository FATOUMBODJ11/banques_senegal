"""
Script 01 - Ingestion de base_senegal2.xlsx dans MongoDB
Collections créées :
  - banques       : infos statiques par banque
  - indicateurs   : données financières annuelles
"""

import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "banques_senegal")
EXCEL_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "base_senegal2.xlsx")

# ── Renommage des colonnes (noms propres sans accents ni points) ─────────────
RENAME = {
    "Sigle"                : "sigle",
    "Goupe_Bancaire"       : "groupe_bancaire",
    "ANNEE"                : "annee",
    "EMPLOI"               : "emploi",
    "BILAN"                : "bilan",
    "RESSOURCES"           : "ressources",
    "FONDS.PROPRE"         : "fonds_propre",
    "EFFECTIF"             : "effectif",
    "AGENCE"               : "agence",
    "COMPTE"               : "compte",
    "INTERETS.ET.PRODUITS.ASSIMILES"                                                              : "interets_produits",
    "NTERETS.ET.CHARGES.ASSIMILEES"                                                               : "interets_charges",
    "REVENUS.DES.TITRES.A.REVENU.VARIABLE"                                                        : "revenus_titres",
    "COMMISSIONS.(PRODUITS)"                                                                      : "commissions_produits",
    "COMMISSIONS.(CHARGES)"                                                                       : "commissions_charges",
    "GAINS.OU.PERTES.NETS.SUR.OPERATIONS.DES.PORTEFEUILLES.DE.NEGOCIATION"                        : "gains_pertes_negociation",
    "GAINS.OU.PERTES.NETS.SUR.OPERATIONS.DES.PORTEFEUILLES.DE.PLACEMENT.ET.ASSIMILES"             : "gains_pertes_placement",
    "AUTRES.PRODUITS.D'EXPLOITATION.BANCAIRE"                                                     : "autres_produits_exploitation",
    "AUTRES.CHARGES.D'EXPLOITATION.BANCAIRE"                                                      : "autres_charges_exploitation",
    "PRODUIT.NET.BANCAIRE"                                                                        : "produit_net_bancaire",
    "SUBVENTIONS.D'INVESTISSEMENT"                                                                : "subventions_investissement",
    "CHARGES.GENERALES.D'EXPLOITATION"                                                            : "charges_generales_exploitation",
    "DOTATIONS.AUX.AMORTISSEMENTS.ET.AUX.DEPRECIATIONS.DES.IMMOBILISATIONS.INCORPORELLES.ET.CORPORELLES": "dotations_amortissements",
    "RESULTAT.BRUT.D'EXPLOITATION"                                                                : "resultat_brut_exploitation",
    "COÛT.DU.RISQUE"                                                                              : "cout_du_risque",
    "RESULTAT.D'EXPLOITATION"                                                                     : "resultat_exploitation",
    "GAINS.OU.PERTES.NETS.SUR.ACTIFS.IMMOBILISES"                                                 : "gains_pertes_actifs_immobilises",
    "RESULTAT.AVANT.IMPÔT"                                                                        : "resultat_avant_impot",
    "IMPÔTS.SUR.LES.BENEFICES"                                                                    : "impots_benefices",
    "RESULTAT.NET"                                                                                : "resultat_net",
}

COLONNES_NUMERIQUES = [
    "emploi", "bilan", "ressources", "fonds_propre", "effectif", "agence", "compte",
    "interets_produits", "interets_charges", "revenus_titres",
    "commissions_produits", "commissions_charges",
    "gains_pertes_negociation", "gains_pertes_placement",
    "autres_produits_exploitation", "autres_charges_exploitation",
    "produit_net_bancaire", "subventions_investissement",
    "charges_generales_exploitation", "dotations_amortissements",
    "resultat_brut_exploitation", "cout_du_risque", "resultat_exploitation",
    "gains_pertes_actifs_immobilises", "resultat_avant_impot",
    "impots_benefices", "resultat_net",
]


def nettoyer_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=RENAME)

    # Convertir les colonnes numériques (certaines sont 'object' à cause de valeurs texte)
    for col in COLONNES_NUMERIQUES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remplacer NaN par None pour MongoDB
    df = df.where(pd.notnull(df), None)

    # Normaliser les chaînes
    df["sigle"] = df["sigle"].str.strip().str.upper()
    df["groupe_bancaire"] = df["groupe_bancaire"].str.strip()

    return df


def creer_collection_banques(df: pd.DataFrame) -> list:
    """Une ligne par banque avec ses métadonnées."""
    banques = (
        df[["sigle", "groupe_bancaire"]]
        .drop_duplicates(subset="sigle")
        .sort_values("sigle")
    )
    docs = banques.to_dict(orient="records")
    return docs


def creer_collection_indicateurs(df: pd.DataFrame) -> list:
    """Une ligne par banque × année — toutes les données financières."""
    docs = df.to_dict(orient="records")
    return docs


def inserer_dans_mongo(docs_banques: list, docs_indicateurs: list):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # ── Collection banques ──────────────────────────────────────────────────
    col_banques = db["banques"]
    col_banques.drop()
    col_banques.insert_many(docs_banques)
    col_banques.create_index([("sigle", ASCENDING)], unique=True)
    print(f"✅  Collection 'banques'     : {col_banques.count_documents({})} documents insérés")

    # ── Collection indicateurs ─────────────────────────────────────────────
    col_indic = db["indicateurs"]
    col_indic.drop()
    col_indic.insert_many(docs_indicateurs)
    col_indic.create_index([("sigle", ASCENDING), ("annee", ASCENDING)], unique=True)
    print(f"✅  Collection 'indicateurs' : {col_indic.count_documents({})} documents insérés")

    client.close()


def main():
    print("📂  Lecture du fichier Excel...")
    df = pd.read_excel(EXCEL_PATH)
    print(f"    {len(df)} lignes × {len(df.columns)} colonnes chargées")

    print("\n🔧  Nettoyage des données...")
    df = nettoyer_dataframe(df)

    print("\n📊  Résumé après nettoyage :")
    print(f"    Banques  : {df['sigle'].nunique()} — {sorted(df['sigle'].unique())}")
    print(f"    Années   : {sorted(df['annee'].unique())}")
    print(f"    Groupes  : {df['groupe_bancaire'].unique()}")

    docs_banques     = creer_collection_banques(df)
    docs_indicateurs = creer_collection_indicateurs(df)

    print(f"\n🗄️   Connexion à MongoDB ({MONGO_URI}) — base '{DB_NAME}'...")
    inserer_dans_mongo(docs_banques, docs_indicateurs)

    print("\n🎉  Ingestion terminée avec succès !")


if __name__ == "__main__":
    main()