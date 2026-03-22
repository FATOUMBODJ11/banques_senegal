"""
Script 02 - Web scraping + téléchargement des PDF BCEAO
Site : https://www.bceao.int/fr/publications
Ce script :
  1. Scrape la page BCEAO pour trouver les liens vers les rapports PDF
  2. Télécharge chaque PDF dans data/pdf/
  3. Extrait les tableaux financiers avec pdfplumber
  4. Sauvegarde les données brutes en JSON dans data/json_brut/
"""

import os
import json
import time
import requests
import pdfplumber
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_DIR     = os.path.join(os.path.dirname(__file__), "..")
PDF_DIR      = os.path.join(BASE_DIR, "data", "pdf")
JSON_DIR     = os.path.join(BASE_DIR, "data", "json_brut")

os.makedirs(PDF_DIR,  exist_ok=True)
os.makedirs(JSON_DIR, exist_ok=True)

# URL de la page BCEAO listant les rapports bancaires
URL_BCEAO = "https://www.bceao.int/fr/publications/bilans-et-comptes-de-resultat-des-systemes-financiers-decentralises-de-lumoa-2022"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Mots-clés pour filtrer les liens pertinents
MOTS_CLES = ["senegal", "sénégal", "rapport", "banque", "financier"]


# ── 1. Scraping de la page BCEAO ────────────────────────────────────────────

def scraper_liens_pdf(url: str) -> list:
    """Récupère tous les liens PDF depuis la page BCEAO."""
    print(f"🌐  Connexion à : {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"❌  Erreur de connexion : {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    liens = []

    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        texte = tag.get_text(strip=True).lower()

        # Garder uniquement les liens PDF ou contenant les mots-clés
        if href.endswith(".pdf") or any(mot in texte for mot in MOTS_CLES):
            lien_complet = href if href.startswith("http") else "https://www.bceao.int" + href
            liens.append({
                "url":   lien_complet,
                "texte": tag.get_text(strip=True)
            })

    print(f"    {len(liens)} liens trouvés")
    return liens


# ── 2. Téléchargement des PDF ───────────────────────────────────────────────

def telecharger_pdf(lien: dict) -> str | None:
    """Télécharge un PDF et retourne son chemin local."""
    url   = lien["url"]
    nom   = url.split("/")[-1].split("?")[0]
    if not nom.endswith(".pdf"):
        nom += ".pdf"
    chemin = os.path.join(PDF_DIR, nom)

    if os.path.exists(chemin):
        print(f"    ⏭️   Déjà téléchargé : {nom}")
        return chemin

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        resp.raise_for_status()
        with open(chemin, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"    ✅  Téléchargé : {nom}")
        time.sleep(1)  # Pause pour ne pas surcharger le serveur
        return chemin
    except Exception as e:
        print(f"    ❌  Échec {nom} : {e}")
        return None


# ── 3. Extraction des données depuis les PDF ────────────────────────────────

def extraire_tableaux_pdf(chemin_pdf: str) -> dict:
    """
    Extrait le texte et les tableaux d'un PDF avec pdfplumber.
    Retourne un dict avec les pages et tableaux trouvés.
    """
    nom_fichier = os.path.basename(chemin_pdf)
    resultat = {
        "fichier": nom_fichier,
        "pages":   [],
        "tableaux": []
    }

    try:
        with pdfplumber.open(chemin_pdf) as pdf:
            print(f"    📄  {nom_fichier} — {len(pdf.pages)} pages")

            for num_page, page in enumerate(pdf.pages, start=1):
                # Extraction du texte brut
                texte = page.extract_text() or ""

                # Extraction des tableaux
                tableaux_page = page.extract_tables()

                resultat["pages"].append({
                    "numero": num_page,
                    "texte":  texte[:2000]  # Limiter à 2000 caractères par page
                })

                for i, tableau in enumerate(tableaux_page):
                    if tableau and len(tableau) > 1:
                        # Nettoyer le tableau : enlever les cellules None
                        tableau_propre = [
                            [str(cell).strip() if cell else "" for cell in ligne]
                            for ligne in tableau
                        ]
                        resultat["tableaux"].append({
                            "page":    num_page,
                            "index":   i,
                            "donnees": tableau_propre
                        })

    except Exception as e:
        print(f"    ❌  Erreur lecture PDF {nom_fichier} : {e}")

    return resultat


def identifier_donnees_senegal(extraction: dict) -> dict:
    """
    Filtre les pages et tableaux qui concernent le Sénégal.
    """
    mots_senegal = ["sénégal", "senegal", "dakar", "SN"]

    pages_senegal   = []
    tableaux_senegal = []

    for page in extraction["pages"]:
        texte_lower = page["texte"].lower()
        if any(mot.lower() in texte_lower for mot in mots_senegal):
            pages_senegal.append(page)

    for tableau in extraction["tableaux"]:
        contenu = str(tableau["donnees"]).lower()
        if any(mot.lower() in contenu for mot in mots_senegal):
            tableaux_senegal.append(tableau)

    return {
        "fichier":          extraction["fichier"],
        "pages_senegal":    pages_senegal,
        "tableaux_senegal": tableaux_senegal,
        "nb_pages_total":   len(extraction["pages"]),
        "nb_tableaux_total": len(extraction["tableaux"])
    }


# ── 4. Sauvegarde JSON ──────────────────────────────────────────────────────

def sauvegarder_json(donnees: dict, nom_fichier: str):
    chemin = os.path.join(JSON_DIR, nom_fichier.replace(".pdf", ".json"))
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(donnees, f, ensure_ascii=False, indent=2)
    print(f"    💾  Sauvegardé : {os.path.basename(chemin)}")


# ── MAIN ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  ÉTAPE 2 — Scraping & Extraction PDF BCEAO")
    print("=" * 60)

    # 1. Scraper les liens
    print("\n📡  Recherche des rapports PDF sur le site BCEAO...")
    liens = scraper_liens_pdf(URL_BCEAO)

    if not liens:
        print("\n⚠️   Aucun lien trouvé automatiquement.")
        print("    → Télécharge manuellement les PDF depuis :")
        print("      https://www.bceao.int/fr/publications")
        print(f"    → Place-les dans : {PDF_DIR}")
        print("    → Relance ce script, il traitera les PDF locaux.\n")
    else:
        # 2. Télécharger les PDF
        print(f"\n📥  Téléchargement des PDF dans {PDF_DIR}...")
        for lien in liens[:10]:  # Limiter à 10 PDF pour commencer
            telecharger_pdf(lien)

    # 3. Traiter tous les PDF présents dans data/pdf/
    pdf_locaux = [f for f in os.listdir(PDF_DIR) if f.endswith(".pdf")]

    if not pdf_locaux:
        print("\n📭  Aucun PDF trouvé dans data/pdf/")
        print("    Télécharge les rapports manuellement et relance le script.")
        return

    print(f"\n🔍  Extraction des données de {len(pdf_locaux)} PDF...")
    resultats_globaux = []

    for nom_pdf in pdf_locaux:
        chemin = os.path.join(PDF_DIR, nom_pdf)
        print(f"\n  → Traitement de : {nom_pdf}")

        extraction  = extraire_tableaux_pdf(chemin)
        donnees_sn  = identifier_donnees_senegal(extraction)
        sauvegarder_json(donnees_sn, nom_pdf)
        resultats_globaux.append(donnees_sn)

    # Résumé
    print("\n" + "=" * 60)
    print(f"✅  {len(resultats_globaux)} PDF traités")
    total_tableaux = sum(len(r["tableaux_senegal"]) for r in resultats_globaux)
    print(f"    Tableaux Sénégal trouvés : {total_tableaux}")
    print(f"    JSON sauvegardés dans    : {JSON_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()