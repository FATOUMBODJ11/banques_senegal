"""
insertion_pdf_2020_2022.py
Insère les données extraites du PDF BCEAO (2020/2021/2022)
pour chaque banque dans MongoDB.
Lancer : python scripts/insertion_pdf_2020_2022.py
"""

import pdfplumber, re, json, numpy as np
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "banques_senegal")

PDF_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "pdf_bceao",
                        "bceao_bilans_senegal_2023.pdf")

# Si le PDF n'est pas dans pdf_bceao, essayer uploads
PDF_PATHS = [
    PDF_PATH,
    os.path.join(os.path.dirname(__file__), "..", "data", "pdf_bceao",
                 "Bilans%20et%20comptes%20de%20r%C3%A9sultat%20des%20banques%2C%20%C3%A9tablissements%20financiers%20et%20compagnies%20financi%C3%A8res%20de%20l%27UMOA%202022.pdf"),
]

GROUPES = {
    "BAS":"Groupes Continentaux","BCIM":"Groupes Internationaux",
    "BDK":"Groupes Règionaux","BGFI":"Groupes Continentaux",
    "BHS":"Groupes Locaux","BICIS":"Groupes Internationaux",
    "BIS":"Groupes Règionaux","BNDE":"Groupes Locaux",
    "BOA":"Groupes Règionaux","BRM":"Groupes Locaux",
    "BSIC":"Groupes Règionaux","CBAO":"Groupes Internationaux",
    "CBI":"Groupes Locaux","CDS":"Groupes Locaux",
    "CITIBANK":"Groupes Internationaux","ECOBANK":"Groupes Règionaux",
    "FBNBANK":"Groupes Internationaux","LBA":"Groupes Règionaux",
    "LBO":"Groupes Locaux","NSIA Banque":"Groupes Règionaux",
    "ORABANK":"Groupes Règionaux","SGBS":"Groupes Internationaux",
    "UBA":"Groupes Internationaux",
}

PAGES_BILAN = {
    "SGBS":266,"BICIS":268,"CBAO":270,"CDS":272,"BHS":274,
    "CITIBANK":276,"LBA":278,"BIS":280,"ECOBANK":282,"ORABANK":284,
    "BOA":286,"BSIC":288,"BAS":292,"BRM":294,"UBA":296,
    "FBNBANK":298,"BNDE":302,"NSIA Banque":304,"BDK":306,
    "BGFI":308,"CBI":314,
}
PAGES_RESULTAT = {
    "SGBS":267,"BICIS":269,"CBAO":271,"CDS":273,"BHS":275,
    "CITIBANK":277,"LBA":279,"BIS":281,"ECOBANK":283,"ORABANK":285,
    "BOA":287,"BSIC":289,"BAS":293,"BRM":295,"UBA":297,
    "FBNBANK":299,"BNDE":303,"NSIA Banque":305,"BDK":307,
    "BGFI":309,"CBI":315,
}

ANNEES = [2020, 2021, 2022]

LABELS_BILAN = [
    ("TOTAL DE L\u2019ACTIF",                 "bilan"),
    ("TOTAL DE L'ACTIF",                       "bilan"),
    ("gard de la client",                      "ressources"),
    ("Capitaux propres et ressources assimil", "fonds_propre"),
    ("sultat de l\u2019exercice",              "resultat_net"),
    ("sultat de l'exercice",                   "resultat_net"),
    ("Engagements donn",                       "emploi"),
    ("ances sur la client",                    "creances_clientele"),
]

LABELS_RESULTAT = [
    ("ts et produits assimil",             "interets_produits"),
    ("ts et charges assimil",              "interets_charges"),
    ("Commissions (produits)",             "commissions_produits"),
    ("Commissions (charges)",              "commissions_charges"),
    ("PRODUIT NET BANCAIRE",               "produit_net_bancaire"),
    ("rales d\u2019exploitation",          "charges_generales_exploitation"),
    ("rales d'exploitation",               "charges_generales_exploitation"),
    ("Dotation aux amortissements",        "dotations_amortissements"),
    ("SULTAT BRUT D\u2019EXPLOITATION",    "resultat_brut_exploitation"),
    ("SULTAT BRUT D'EXPLOITATION",         "resultat_brut_exploitation"),
    ("t du risque",                        "cout_du_risque"),
    ("SULTAT D\u2019EXPLOITATION",         "resultat_exploitation"),
    ("SULTAT D'EXPLOITATION",              "resultat_exploitation"),
    ("SULTAT AVANT IMP",                   "resultat_avant_impot"),
    ("ts sur les b",                       "impots_benefices"),
    ("SULTAT NET",                         "resultat_net"),
]


def parse_3(ligne):
    tokens = ligne.split()
    s = len(tokens)
    for i in range(len(tokens)-1, -1, -1):
        if re.match(r'^-?\d+$', tokens[i]): s = i
        else: break
    nt = tokens[s:]
    if not nt: return None

    def val(g):
        nb = ''.join(t.lstrip('-') for t in g)
        neg = g[0].startswith('-')
        try: v = float(nb); return -v if neg else v
        except: return None

    n = len(nt)
    if n == 3: return [val([t]) for t in nt]
    for seuil in [2_000_000, 20_000_000]:
        for i in range(1, n-1):
            for j in range(i+1, n):
                v1,v2,v3 = val(nt[:i]),val(nt[i:j]),val(nt[j:])
                if None not in (v1,v2,v3) and all(abs(x)<=seuil for x in (v1,v2,v3)):
                    return [v1,v2,v3]
    return None


def calculer_ratios(doc):
    d = doc.copy()
    def r(a, b):
        try:
            av, bv = d.get(a), d.get(b)
            if av is None or bv is None or bv == 0: return None
            return round(av / bv * 100, 4)
        except: return None
    d["ratio_roa"]          = r("resultat_net", "bilan")
    d["ratio_roe"]          = r("resultat_net", "fonds_propre")
    d["ratio_solvabilite"]  = r("fonds_propre", "bilan")
    d["ratio_exploitation"] = r("charges_generales_exploitation", "produit_net_bancaire")
    d["ratio_marge_nette"]  = r("resultat_net", "produit_net_bancaire")
    return d


def extraire_depuis_pdf(pdf_path):
    tous = {}
    with pdfplumber.open(pdf_path) as pdf:
        for sigle in PAGES_BILAN:
            for pages, labels in [(PAGES_BILAN, LABELS_BILAN),
                                   (PAGES_RESULTAT, LABELS_RESULTAT)]:
                p = pdf.pages[pages[sigle]]
                texte = p.extract_text() or ""
                used_cols = set()
                for ligne in texte.split('\n'):
                    for label, col in labels:
                        if col in used_cols: continue
                        if label.lower() in ligne.lower():
                            nums = parse_3(ligne)
                            if nums and len(nums) == 3:
                                for j, a in enumerate(ANNEES):
                                    k = (sigle, a)
                                    if k not in tous:
                                        tous[k] = {
                                            "sigle": sigle, "annee": a,
                                            "groupe_bancaire": GROUPES.get(sigle, ""),
                                            "source": "pdf_bceao"
                                        }
                                    tous[k][col] = nums[j]
                                used_cols.add(col)
    return list(tous.values())


def main():
    print("=" * 60)
    print("  INSERTION PDF BCEAO 2020/2021/2022 → MongoDB")
    print("=" * 60)

    # Trouver le PDF
    pdf_path = None
    for p in PDF_PATHS:
        if os.path.exists(p):
            pdf_path = p
            break

    if not pdf_path:
        pdf_dir = os.path.join(os.path.dirname(__file__), "..", "data", "pdf_bceao")
        pdfs = [f for f in os.listdir(pdf_dir) if f.endswith(".pdf")]
        if pdfs:
            pdf_path = os.path.join(pdf_dir, pdfs[0])
        else:
            print("❌  PDF non trouvé dans data/pdf_bceao/")
            print("    Place le PDF BCEAO 2022 dans ce dossier et relance.")
            return

    print(f"📄  PDF utilisé : {os.path.basename(pdf_path)}")

    # Extraction
    print("\n🔍  Extraction des données...")
    docs = extraire_depuis_pdf(pdf_path)
    docs = [calculer_ratios(d) for d in docs]

    df = pd.DataFrame(docs)
    print(f"\n✅  {len(docs)} documents extraits")
    print(f"    Sigles  : {sorted(df['sigle'].unique())}")
    print(f"    Années  : {sorted(df['annee'].unique())}")

    # Aperçu
    cols = ["sigle","annee","bilan","ressources","fonds_propre",
            "produit_net_bancaire","resultat_net"]
    ok = [c for c in cols if c in df.columns]
    print(f"\n{df[ok].sort_values(['annee','sigle']).to_string()}")

    # MongoDB
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    df_clean = df.where(pd.notnull(df), None)
    inseres  = 0
    for doc in df_clean.to_dict(orient="records"):
        for col in ["indicateurs", "indicateurs_propre"]:
            db[col].update_one(
                {"sigle": doc["sigle"], "annee": doc["annee"]},
                {"$set": doc}, upsert=True
            )
        inseres += 1

    total = db["indicateurs"].count_documents({})
    print(f"\n✅  {inseres} documents insérés dans MongoDB")
    print(f"    Total 'indicateurs' : {total} docs")
    print(f"    Années disponibles  : "
          f"{sorted(set(d['annee'] for d in db['indicateurs'].find({},{'annee':1})))}")
    client.close()

    print(f"\n▶️   Relance le dashboard : python dashboard/app.py")
    print("=" * 60)


if __name__ == "__main__":
    main()