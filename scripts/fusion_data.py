"""
fusion_data.py
Fusion finale + Data Engineering depuis MongoDB.
Ce script :
  1. Charge la collection 'indicateurs_nettoye' (produite par nettoyage.py)
  2. Calcule les ratios financiers
  3. Sauvegarde la collection finale 'indicateurs_propre' dans MongoDB

Ordre d'exécution :
  1. python scripts/insertion_pdf_2020_2022.py   ← extrait le PDF → MongoDB
  2. python scripts/nettoyage.py                 ← nettoie les données
  3. python scripts/fusion_data.py               ← calcule les ratios → indicateurs_propre
  4. python dashboard/app.py                     ← lance le dashboard
"""

import os
import numpy as np
import pandas as pd
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "banques_senegal")


# ── 1. Chargement ─────────────────────────────────────────────────────────────

def charger_depuis_mongo() -> pd.DataFrame:
    """Charge la collection nettoyée produite par nettoyage.py."""
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    docs = list(db["indicateurs_nettoye"].find({}, {"_id": 0}))
    client.close()

    if not docs:
        raise SystemExit(
            "\n❌  Collection 'indicateurs_nettoye' vide ou introuvable.\n"
            "    Lance d'abord : python scripts/nettoyage.py"
        )

    df = pd.DataFrame(docs)
    annees = sorted(df["annee"].dropna().unique())
    print(f"  ✅  {len(df)} documents chargés — années {annees}")
    return df


# ── 2. Calcul des ratios financiers ──────────────────────────────────────────

def calculer_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule tous les ratios financiers."""
    def safe_div(a, b):
        return np.where(
            (b != 0) & b.notna() & a.notna(),
            a / b * 100, np.nan
        )

    # Vérifier que les colonnes nécessaires existent
    required = ["resultat_net", "bilan", "fonds_propre",
                "charges_generales_exploitation", "produit_net_bancaire", "emploi"]
    manquantes = [c for c in required if c not in df.columns]
    if manquantes:
        print(f"  ⚠️   Colonnes absentes pour les ratios : {manquantes}")

    if "resultat_net" in df.columns and "bilan" in df.columns:
        df["ratio_roa"] = safe_div(df["resultat_net"], df["bilan"]).round(4)

    if "resultat_net" in df.columns and "fonds_propre" in df.columns:
        df["ratio_roe"] = safe_div(df["resultat_net"], df["fonds_propre"]).round(4)

    if "fonds_propre" in df.columns and "bilan" in df.columns:
        df["ratio_solvabilite"] = safe_div(df["fonds_propre"], df["bilan"]).round(4)

    if "charges_generales_exploitation" in df.columns and "produit_net_bancaire" in df.columns:
        df["ratio_exploitation"] = safe_div(
            df["charges_generales_exploitation"], df["produit_net_bancaire"]
        ).round(4)

    if "resultat_net" in df.columns and "produit_net_bancaire" in df.columns:
        df["ratio_marge_nette"] = safe_div(
            df["resultat_net"], df["produit_net_bancaire"]
        ).round(4)

    if "produit_net_bancaire" in df.columns and "emploi" in df.columns:
        df["ratio_rendement_emploi"] = safe_div(
            df["produit_net_bancaire"], df["emploi"]
        ).round(4)

    ratios = [c for c in df.columns if c.startswith("ratio_")]
    print(f"  ✅  {len(ratios)} ratios calculés : {ratios}")
    return df


# ── 3. Ordonnancement des colonnes ────────────────────────────────────────────

def trier_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Trie par sigle/annee et ordonne les colonnes proprement."""
    df = df.sort_values(["sigle", "annee"]).reset_index(drop=True)

    cols_ordre = [
        "sigle", "groupe_bancaire", "annee",
        # Bilan
        "bilan", "emploi", "ressources", "fonds_propre",
        "effectif", "agence", "compte",
        # Compte de résultat
        "interets_produits", "interets_charges",
        "commissions_produits", "commissions_charges",
        "produit_net_bancaire", "charges_generales_exploitation",
        "dotations_amortissements", "resultat_brut_exploitation",
        "cout_du_risque", "resultat_exploitation",
        "resultat_avant_impot", "impots_benefices", "resultat_net",
        # Ratios
        "ratio_roa", "ratio_roe", "ratio_solvabilite",
        "ratio_exploitation", "ratio_marge_nette", "ratio_rendement_emploi",
        "source",
    ]
    presentes = [c for c in cols_ordre if c in df.columns]
    autres    = [c for c in df.columns if c not in presentes]
    return df[presentes + autres]


# ── 4. Sauvegarde MongoDB ─────────────────────────────────────────────────────

def sauvegarder_mongo(df: pd.DataFrame):
    """Écrase et recrée la collection 'indicateurs_propre'."""
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    # NaN → None pour MongoDB
    df_clean = df.where(pd.notnull(df), None)
    docs     = df_clean.to_dict(orient="records")

    col = db["indicateurs_propre"]
    col.drop()
    col.insert_many(docs)
    col.create_index([("sigle", ASCENDING), ("annee", ASCENDING)], unique=True)

    print(f"\n  ✅  Collection 'indicateurs_propre' : {col.count_documents({})} documents")
    client.close()


def afficher_bilan(df: pd.DataFrame):
    """Affiche un résumé du dataset final."""
    print("\n" + "=" * 60)
    print("  BILAN DU DATASET FINAL")
    print("=" * 60)
    print(f"  Lignes      : {len(df)}")
    print(f"  Banques     : {df['sigle'].nunique()} — {sorted(df['sigle'].unique())}")
    print(f"  Années      : {sorted(df['annee'].dropna().unique())}")
    print(f"  Colonnes    : {len(df.columns)}")
    print(f"\n  Taux de complétude :")
    for col in ["bilan", "emploi", "produit_net_bancaire",
                "resultat_net", "fonds_propre"]:
        if col in df.columns:
            pct = df[col].notna().mean() * 100
            print(f"    {col:<35} {pct:.1f}%")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  FUSION FINALE + DATA ENGINEERING")
    print("=" * 60)

    print("\n📂  Chargement depuis MongoDB (indicateurs_nettoye)...")
    df = charger_depuis_mongo()

    print("\n📐  Calcul des ratios financiers...")
    df = calculer_ratios(df)
    df = trier_dataset(df)

    print("\n🗄️   Sauvegarde dans MongoDB (indicateurs_propre)...")
    sauvegarder_mongo(df)

    afficher_bilan(df)

    print("\n🎉  Dataset final prêt — lance le dashboard :")
    print("    python dashboard/app.py")
    print("=" * 60)


if __name__ == "__main__":
    main()