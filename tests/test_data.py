"""
tests/test_data.py
==================
Tests unitaires — Intégrité des données MongoDB
Vérifie que la collection 'indicateurs_propre' est correcte
avant de lancer le dashboard.

Lancer : python -m pytest tests/ -v
"""

import pytest
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "banques_senegal")

# Colonnes obligatoires dans la collection
COLS_OBLIGATOIRES = [
    "sigle", "annee", "bilan", "resultat_net",
    "produit_net_bancaire", "groupe_bancaire",
]

# Groupes bancaires valides selon la BCEAO
GROUPES_VALIDES = {
    "Groupes Continentaux",
    "Groupes Règionaux",
    "Groupes Internationaux",
    "Groupes Locaux",
}

# Banques attendues
BANQUES_ATTENDUES = {
    "BAS","BCIM","BDK","BGFI","BHS","BICIS","BIS","BNDE",
    "BOA","BRM","BSIC","CBAO","CBI","CDS","CITIBANK",
    "ECOBANK","FBNBANK","LBA","LBO","NSIA Banque",
    "ORABANK","SGBS","UBA",
}


# ── Fixture : chargement unique du DataFrame ─────────────────────────────────

@pytest.fixture(scope="module")
def df():
    """Charge la collection MongoDB une seule fois pour tous les tests."""
    client = MongoClient(MONGO_URI)
    data = list(client[DB_NAME]["indicateurs_propre"].find({}, {"_id": 0}))
    client.close()
    assert len(data) > 0, (
        "❌ La collection 'indicateurs_propre' est vide. "
        "Lance d'abord : python scripts/fusion_data.py"
    )
    return pd.DataFrame(data)


# ── Tests de base ─────────────────────────────────────────────────────────────

def test_collection_non_vide(df):
    """La collection doit contenir au moins 100 documents."""
    assert len(df) >= 100, (
        f"❌ Seulement {len(df)} documents — attendu ≥ 100"
    )


def test_colonnes_obligatoires_presentes(df):
    """Toutes les colonnes critiques doivent être présentes."""
    for col in COLS_OBLIGATOIRES:
        assert col in df.columns, f"❌ Colonne manquante : '{col}'"


def test_pas_de_sigle_nul(df):
    """Aucun sigle ne doit être nul ou vide."""
    nuls = df["sigle"].isna().sum()
    assert nuls == 0, f"❌ {nuls} sigle(s) nul(s) détecté(s)"


def test_pas_de_sigle_vide(df):
    """Aucun sigle ne doit être une chaîne vide."""
    vides = (df["sigle"].astype(str).str.strip() == "").sum()
    assert vides == 0, f"❌ {vides} sigle(s) vide(s) détecté(s)"


# ── Tests années ──────────────────────────────────────────────────────────────

def test_annees_valides(df):
    """Toutes les années doivent être entre 2010 et 2025."""
    annees = pd.to_numeric(df["annee"], errors="coerce").dropna()
    invalides = annees[(annees < 2010) | (annees > 2025)]
    assert len(invalides) == 0, (
        f"❌ Années invalides détectées : {invalides.unique().tolist()}"
    )


def test_annees_attendues_presentes(df):
    """Les années 2015 à 2020 doivent être présentes (données Excel)."""
    annees_en_base = set(df["annee"].dropna().astype(int).unique())
    for annee in range(2015, 2021):
        assert annee in annees_en_base, (
            f"❌ Année {annee} manquante dans la base"
        )


# ── Tests valeurs financières ─────────────────────────────────────────────────

def test_bilan_positif(df):
    """Tous les bilans renseignés doivent être strictement positifs."""
    bilans = pd.to_numeric(df["bilan"], errors="coerce").dropna()
    negatifs = bilans[bilans <= 0]
    assert len(negatifs) == 0, (
        f"❌ {len(negatifs)} bilan(s) négatif(s) ou nul(s) détecté(s)"
    )


def test_bilan_completude(df):
    """Le bilan doit être renseigné pour au moins 90% des lignes."""
    pct = df["bilan"].notna().mean() * 100
    assert pct >= 90, (
        f"❌ Taux de complétude du bilan trop faible : {pct:.1f}% (min 90%)"
    )


def test_pnb_completude(df):
    """Le PNB doit être renseigné pour au moins 80% des lignes."""
    pct = df["produit_net_bancaire"].notna().mean() * 100
    assert pct >= 80, (
        f"❌ Taux de complétude du PNB trop faible : {pct:.1f}% (min 80%)"
    )


def test_resultat_net_completude(df):
    """Le résultat net doit être renseigné pour au moins 80% des lignes."""
    pct = df["resultat_net"].notna().mean() * 100
    assert pct >= 80, (
        f"❌ Taux de complétude du résultat net trop faible : {pct:.1f}%"
    )


# ── Tests groupes bancaires ───────────────────────────────────────────────────

def test_groupes_connus(df):
    """Tous les groupes bancaires doivent faire partie des groupes valides."""
    groupes_en_base = set(df["groupe_bancaire"].dropna().unique())
    inattendus = groupes_en_base - GROUPES_VALIDES
    assert not inattendus, (
        f"❌ Groupes bancaires inconnus : {inattendus}"
    )


def test_pas_de_groupe_nul(df):
    """Aucune banque ne doit avoir un groupe bancaire nul."""
    nuls = df["groupe_bancaire"].isna().sum()
    assert nuls == 0, (
        f"❌ {nuls} ligne(s) sans groupe bancaire"
    )


# ── Tests banques ─────────────────────────────────────────────────────────────

def test_nombre_minimum_banques(df):
    """Au moins 20 banques distinctes doivent être présentes."""
    nb = df["sigle"].nunique()
    assert nb >= 20, (
        f"❌ Seulement {nb} banques distinctes — attendu ≥ 20"
    )


def test_banques_principales_presentes(df):
    """Les grandes banques (SGBS, CBAO, ECOBANK, BOA) doivent être présentes."""
    banques_en_base = set(df["sigle"].unique())
    principales = {"SGBS", "CBAO", "ECOBANK", "BOA", "BICIS"}
    manquantes = principales - banques_en_base
    assert not manquantes, (
        f"❌ Banques principales manquantes : {manquantes}"
    )


# ── Tests ratios financiers ───────────────────────────────────────────────────

def test_ratio_roa_plage(df):
    """Le ROA doit être dans la plage [-50%, +50%]."""
    if "ratio_roa" not in df.columns:
        pytest.skip("Colonne ratio_roa absente")
    roa = pd.to_numeric(df["ratio_roa"], errors="coerce").dropna()
    hors_plage = roa[(roa < -50) | (roa > 50)]
    assert len(hors_plage) == 0, (
        f"❌ {len(hors_plage)} valeur(s) ROA hors plage [-50, 50]"
    )



def test_ratio_solvabilite_plage(df):
    """
    La solvabilité doit être dans la plage [-100%, +100%].
    Note : BRM 2020 a des fonds propres négatifs (-71 155 M FCFA),
    situation réelle de détresse financière — valeur conservée.
    """
    if "ratio_solvabilite" not in df.columns:
        pytest.skip("Colonne ratio_solvabilite absente")
    solv = pd.to_numeric(df["ratio_solvabilite"], errors="coerce").dropna()
    hors_plage = solv[(solv < -100) | (solv > 100)]
    assert len(hors_plage) == 0, (
        f"❌ {len(hors_plage)} valeur(s) de solvabilité hors plage [-100, 100] : "
        f"{hors_plage.values.tolist()}"
    )


def test_pas_de_doublons(df):
    """Chaque combinaison sigle + année doit être unique."""
    doublons = df.duplicated(subset=["sigle", "annee"]).sum()
    assert doublons == 0, (
        f"❌ {doublons} doublon(s) détecté(s) (même sigle + même année)"
    )


# ── Résumé affiché en fin de tests ───────────────────────────────────────────

def test_afficher_resume(df):
    """Affiche un résumé de la base — ce test passe toujours."""
    print("\n")
    print("=" * 50)
    print("  RÉSUMÉ DE LA BASE DE DONNÉES")
    print("=" * 50)
    print(f"  Documents total  : {len(df)}")
    print(f"  Banques          : {df['sigle'].nunique()}")
    print(f"  Années           : {sorted(df['annee'].dropna().astype(int).unique())}")
    print(f"  Groupes          : {sorted(df['groupe_bancaire'].dropna().unique())}")
    print("=" * 50)
    assert True