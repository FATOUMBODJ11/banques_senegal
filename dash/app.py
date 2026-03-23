"""
Dashboard — Positionnement des Banques au Sénégal
Design moderne : header dégradé, KPI cards colorées, barres dégradées
Lancer : python dash/app.py
"""

import warnings
warnings.filterwarnings("ignore")

import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import cm
import os, io

pio.templates.default = "plotly"
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "banques_senegal")

def charger():
    client = MongoClient(MONGO_URI)
    df = pd.DataFrame(list(client[DB_NAME]["indicateurs_propre"].find({}, {"_id": 0})))
    client.close()
    return df

df = charger()
if df.empty:
    raise SystemExit(f"\n❌ Collection MongoDB vide — Base: {DB_NAME}\n")

def calc_ratios(df):
    d = df.copy()
    def r(a, b):
        return np.where((b != 0) & b.notna() & a.notna(), a / b * 100, np.nan)
    d["ratio_roa"]          = np.round(r(d["resultat_net"], d["bilan"]), 2)
    d["ratio_roe"]          = np.round(r(d["resultat_net"], d["fonds_propre"]), 2)
    d["ratio_solvabilite"]  = np.round(r(d["fonds_propre"], d["bilan"]), 2)
    d["ratio_exploitation"] = np.round(r(d["charges_generales_exploitation"], d["produit_net_bancaire"]), 2)
    d["ratio_marge_nette"]  = np.round(r(d["resultat_net"], d["produit_net_bancaire"]), 2)
    return d

df = calc_ratios(df)
ANNEES      = sorted(df["annee"].unique())
BANQUES     = sorted(df["sigle"].unique())
GROUPES     = sorted(df["groupe_bancaire"].unique())
BANQUES_PDF = [b for b in BANQUES if b != "SENEGAL_AGG"]
ANNEE_DEF   = max(ANNEES)

COORDS = {
    "CBAO":(14.6937,-17.4441),"SGBS":(14.6928,-17.4467),
    "BICIS":(14.6915,-17.4382),"ECOBANK":(14.7167,-17.4677),
    "BOA":(14.6892,-17.4356),"CITIBANK":(14.7234,-17.4689),
    "UBA":(14.6956,-17.4512),"ORABANK":(14.6843,-17.4298),
    "BHS":(14.6878,-17.4423),"BAS":(14.7012,-17.4534),
    "BNDE":(14.6967,-17.4478),"BRM":(14.6823,-17.4367),
    "BGFI":(14.7089,-17.4601),"BDK":(14.6901,-17.4445),
    "BCIM":(14.6834,-17.4312),"BIS":(14.6978,-17.4489),
    "BSIC":(14.7045,-17.4556),"CBI":(14.6856,-17.4334),
    "CDS":(14.6789,-17.4278),"CISA":(14.7123,-17.4623),
    "FBNBANK":(14.6912,-17.4401),"LBA":(14.6867,-17.4389),
    "LBO":(14.7001,-17.4512),"NSIA Banque":(14.6945,-17.4467),
}

PALETTE = [
    "#FF6B9D","#FFD93D","#6BCB77","#4D96FF","#FF9A3C",
    "#C77DFF","#00C9A7","#FF6B6B","#48CAE4","#F4A261",
]

# ── CSS ───────────────────────────────────────────────────────────────────────
CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:#f0f4f8;font-family:'Inter',sans-serif;color:#1e293b;font-size:14px}

/* ── HEADER ── */
#hdr{
  background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 60%,#0f172a 100%);
  padding:16px 28px;display:flex;align-items:center;justify-content:space-between;
  position:sticky;top:0;z-index:200;
}
#hdr-ico{
  width:40px;height:40px;background:#2563eb;border-radius:10px;
  display:flex;align-items:center;justify-content:center;font-size:18px;
  box-shadow:0 4px 14px rgba(37,99,235,.5);flex-shrink:0;
}
#hdr-t{font-size:15px;font-weight:700;color:#fff}
#hdr-s{font-size:10px;color:rgba(255,255,255,.4);margin-top:2px}
#hdr-nav{
  display:flex;gap:2px;background:rgba(255,255,255,.08);
  border-radius:8px;padding:3px;
}
.hn{
  padding:6px 13px;border-radius:6px;border:none;cursor:pointer;
  font-size:11px;font-weight:500;background:transparent;
  color:rgba(255,255,255,.5);font-family:'Inter',sans-serif;transition:all .15s;
}
.hn.on{background:#2563eb;color:#fff;box-shadow:0 2px 8px rgba(37,99,235,.4)}
.hn:hover:not(.on){color:#fff;background:rgba(255,255,255,.1)}
#pdf-wrap{display:flex;align-items:center;gap:10px}

/* ── FILTER BAR ── */
#fbar{
  background:#fff;border-bottom:1px solid #e2e8f0;
  padding:10px 28px;display:flex;align-items:flex-end;gap:24px;
  box-shadow:0 1px 4px rgba(0,0,0,.04);
}
.fl{font-size:9px;font-weight:700;color:#94a3b8;text-transform:uppercase;
    letter-spacing:.08em;margin-bottom:5px}
.yr-wrap{display:flex;align-items:center;gap:10px}
.yr-num{
  font-size:22px;font-weight:800;color:#2563eb;
  font-family:monospace;min-width:52px;
}
.grp-pills{display:flex;gap:5px;flex-wrap:wrap;padding-bottom:2px}
.gpill{
  padding:4px 11px;border-radius:20px;border:1px solid #e2e8f0;
  font-size:10px;font-weight:500;cursor:pointer;color:#64748b;
  background:#f8fafc;transition:all .15s;
}
.gpill.on{background:#2563eb;border-color:#2563eb;color:#fff}
.gpill:hover:not(.on){border-color:#2563eb;color:#2563eb}

/* ── KPI ROW ── */
#kpi-row{
  display:grid;grid-template-columns:repeat(6,1fr);
  gap:10px;padding:16px 28px 0;
}
.kc{
  background:#fff;border-radius:12px;padding:14px 16px;
  border:1px solid #e2e8f0;position:relative;overflow:hidden;
  transition:transform .15s,box-shadow .15s;
}
.kc:hover{transform:translateY(-2px);box-shadow:0 6px 20px rgba(0,0,0,.08)}
.kc-bar{position:absolute;top:0;left:0;right:0;height:3px;border-radius:12px 12px 0 0}
.kl{font-size:9px;font-weight:700;color:#94a3b8;text-transform:uppercase;
    letter-spacing:.07em;margin:8px 0 4px}
.kv{font-size:17px;font-weight:800;line-height:1}
.ki{position:absolute;top:10px;right:10px;font-size:18px;opacity:.14}

/* ── CONTENT ── */
#content{padding:16px 28px 32px}

/* ── TABS ── */
.tab-pg{display:none}
.tab-pg.on{display:block;animation:pIn .2s ease}
@keyframes pIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}

/* ── GRAPH CARDS ── */
.gc{
  background:#fff;border-radius:12px;border:1px solid #e2e8f0;
  padding:16px;height:100%;
  transition:box-shadow .15s;
}
.gc:hover{box-shadow:0 4px 16px rgba(0,0,0,.07)}
.gct{
  font-size:11px;font-weight:700;color:#334155;
  margin-bottom:12px;padding-bottom:10px;border-bottom:1px solid #f1f5f9;
  display:flex;align-items:center;gap:7px;
}
.gcd{width:6px;height:6px;background:#2563eb;border-radius:50%;flex-shrink:0}
.gcs{font-size:9px;color:#94a3b8;font-weight:400;margin-left:auto}

/* ── DROPDOWNS ── */
.Select-control{
  background:#fff !important;border:1px solid #e2e8f0 !important;
  border-radius:8px !important;min-height:34px !important;
}
.Select-control:hover{border-color:#2563eb !important}
.Select-value-label{color:#1e293b !important;font-family:'Inter',sans-serif !important;font-size:12px !important}
.Select-placeholder{color:#94a3b8 !important;font-size:12px !important}
.Select-menu-outer{
  background:#fff !important;border:1px solid #e2e8f0 !important;
  border-radius:8px !important;box-shadow:0 8px 24px rgba(0,0,0,.12) !important;
}
.Select-option{color:#1e293b !important;font-size:12px !important;font-family:'Inter',sans-serif !important}
.Select-option:hover,.Select-option.is-focused{background:#eff6ff !important;color:#2563eb !important}
.Select-option.is-selected{background:#2563eb !important;color:#fff !important}
.Select-multi-value-wrapper .Select-value{
  background:#eff6ff !important;border:1px solid #bfdbfe !important;
  border-radius:4px !important;
}
.Select-multi-value-wrapper .Select-value-label{color:#2563eb !important;font-size:11px !important}
.Select-multi-value-wrapper .Select-value-icon{border-right:1px solid #bfdbfe !important;color:#2563eb !important}

/* ── SLIDER ── */
.rc-slider-track{background:#2563eb !important;height:4px !important}
.rc-slider-rail{background:#e2e8f0 !important;height:4px !important}
.rc-slider-handle{
  border:2px solid #2563eb !important;background:#fff !important;
  width:14px !important;height:14px !important;margin-top:-5px !important;
  box-shadow:0 0 0 3px rgba(37,99,235,.15) !important;
}
.rc-slider-mark-text{color:#94a3b8 !important;font-size:10px !important;font-family:'Inter',sans-serif !important}

/* ── PDF BUTTON ── */
#btn-pdf{
  background:#2563eb !important;border:none !important;
  border-radius:8px !important;font-weight:600 !important;
  font-size:11px !important;padding:8px 16px !important;
  color:#fff !important;transition:all .15s !important;
  box-shadow:0 2px 8px rgba(37,99,235,.35) !important;
  white-space:nowrap !important;
}
#btn-pdf:hover{background:#1d4ed8 !important;transform:translateY(-1px) !important}

/* ── TABLE ── */
.dash-spreadsheet-container .dash-spreadsheet-inner th{
  background:#0f172a !important;color:#fff !important;
  font-family:'Inter',sans-serif !important;font-size:10px !important;
  font-weight:600 !important;text-transform:uppercase !important;
  letter-spacing:.05em !important;padding:10px 8px !important;
}
.dash-spreadsheet-container .dash-spreadsheet-inner td{
  font-family:'Inter',sans-serif !important;font-size:11px !important;
  padding:7px 8px !important;
}
.dash-spreadsheet-container .dash-spreadsheet-inner tr:nth-child(even) td{
  background:#f8fafc !important;
}
"""

# ── PDF ───────────────────────────────────────────────────────────────────────
def generer_pdf(sigle, annee):
    dff = df[(df["sigle"]==sigle)&(df["annee"]==annee)]
    if dff.empty: return None
    row = dff.iloc[0]
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            rightMargin=2*cm, leftMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)
    st = getSampleStyleSheet()
    def v(c, u="M FCFA"):
        x = row.get(c)
        return "—" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:,.0f} {u}"
    def p(c):
        x = row.get(c)
        return "—" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:.2f}%"
    def T(data, hcol):
        t = Table(data, colWidths=[9*cm, 7*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),colors.HexColor(hcol)),
            ("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),9),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.HexColor("#F8FAFC"),colors.white]),
            ("GRID",(0,0),(-1,-1),0.4,colors.HexColor("#E2E8F0")),
            ("PADDING",(0,0),(-1,-1),5),
        ]))
        return t
    s_t = ParagraphStyle("t",parent=st["Title"],fontSize=18,textColor=colors.HexColor("#0f172a"),spaceAfter=4)
    s_s = ParagraphStyle("s",parent=st["Normal"],fontSize=11,textColor=colors.HexColor("#64748b"),spaceAfter=16)
    s_h = ParagraphStyle("h",parent=st["Heading2"],fontSize=12,textColor=colors.HexColor("#2563eb"),spaceBefore=12,spaceAfter=4)
    content = [
        Paragraph(f"Rapport de positionnement — {sigle}", s_t),
        Paragraph(f"Exercice {int(annee)} | Groupe : {row.get('groupe_bancaire','—')}", s_s),
        Paragraph("1. Bilan", s_h),
        T([["Indicateur","Valeur"],
           ["Bilan total",v("bilan")],["Emplois",v("emploi")],
           ["Ressources",v("ressources")],["Fonds propres",v("fonds_propre")],
           ["Effectif",f"{int(row.get('effectif') or 0)} agents"],
           ["Agences",f"{int(row.get('agence') or 0)} agences"]],"#0f172a"),
        Spacer(1,0.3*cm),
        Paragraph("2. Compte de Résultat", s_h),
        T([["Indicateur","Valeur"],
           ["PNB",v("produit_net_bancaire")],
           ["Charges exploitation",v("charges_generales_exploitation")],
           ["Résultat brut",v("resultat_brut_exploitation")],
           ["Coût du risque",v("cout_du_risque")],
           ["Résultat net",v("resultat_net")]],"#2563eb"),
        Spacer(1,0.3*cm),
        Paragraph("3. Ratios Financiers", s_h),
        T([["Ratio","Valeur"],
           ["ROA",p("ratio_roa")],["ROE",p("ratio_roe")],
           ["Solvabilité",p("ratio_solvabilite")],
           ["Coeff. exploitation",p("ratio_exploitation")],
           ["Marge nette",p("ratio_marge_nette")]],"#059669"),
    ]
    doc.build(content)
    buf.seek(0)
    return buf.read()

# ── App ───────────────────────────────────────────────────────────────────────
app = dash.Dash(__name__,
                external_stylesheets=[dbc.themes.BOOTSTRAP],
                title="Banques Sénégal — BCEAO",
                suppress_callback_exceptions=True)

app.index_string = f"""<!DOCTYPE html>
<html>
<head>
{{%metas%}}
<title>{{%title%}}</title>
{{%favicon%}}
{{%css%}}
<style>{CSS}</style>
</head>
<body>
{{%app_entry%}}
<footer>{{%config%}}{{%scripts%}}{{%renderer%}}</footer>
</body>
</html>"""

TABS = [
    ("t1","📊","Bilan"),("t2","💹","Résultats"),("t3","⚖️","Ratios"),
    ("t4","📈","Évolution"),("t5","🗺️","Carte"),("t6","🏆","Comparaison"),("t7","📋","Tableau"),
]

KPI_CONF = [
    ("k-b","Total Bilan","#2563eb","📊"),
    ("k-e","Total Emplois","#0891b2","💼"),
    ("k-r","Ressources","#059669","🏦"),
    ("k-f","Fonds propres","#d97706","🛡️"),
    ("k-roa","ROA moyen","#7c3aed","📈"),
    ("k-p","PNB moyen","#0f172a","💰"),
]

def gc(title, gid, width=6, sub=""):
    return dbc.Col(html.Div([
        html.Div([
            html.Span(className="gcd"),
            html.Span(title),
            html.Span(sub, className="gcs"),
        ], className="gct"),
        dcc.Graph(id=gid, config={"displayModeBar":False}),
    ], className="gc"), width=width)

# ── Layout ────────────────────────────────────────────────────────────────────
app.layout = html.Div([

    # Header
    html.Div([
        html.Div([
            html.Div("🏦", id="hdr-ico"),
            html.Div([
                html.Div("Positionnement des Banques au Sénégal", id="hdr-t"),
                html.Div(f"Analyse comparative · Base BCEAO · {min(ANNEES)}–{max(ANNEES)}", id="hdr-s"),
            ])
        ], style={"display":"flex","alignItems":"center","gap":"14px"}),

        html.Div([
            html.Button(
                [html.Span(ico, style={"marginRight":"5px"}), lbl],
                id=f"nav-{tid}", n_clicks=0,
                className=f"hn{'  on' if i==0 else ''}"
            )
            for i,(tid,ico,lbl) in enumerate(TABS)
        ], id="hdr-nav"),

        html.Div([
            dcc.Dropdown(id="pdf-b",
                options=[{"label":b,"value":b} for b in BANQUES_PDF],
                value=BANQUES_PDF[0], clearable=False,
                style={"width":"130px","fontSize":"11px"}),
            dbc.Button("📄 Rapport PDF", id="btn-pdf", n_clicks=0),
            dcc.Download(id="dl-pdf"),
        ], id="pdf-wrap"),
    ], id="hdr"),

    # Filter bar
    html.Div([
        html.Div([
            html.Div("📅 Année", className="fl"),
            html.Div([
                dcc.Dropdown(
                    id="fa",
                    options=[{"label":str(int(a)),"value":int(a)} for a in ANNEES],
                    value=int(ANNEE_DEF),
                    clearable=False,
                    style={"width":"110px","fontSize":"14px","fontWeight":"700"},
                ),
                html.Div(str(ANNEE_DEF), id="yr-disp", className="yr-num"),
            ], className="yr-wrap"),
        ]),
        html.Div([
            html.Div("🏛️ Groupe bancaire", className="fl"),
            html.Div([
                html.Span("Tous", id="gpill-tous", n_clicks=0,
                          className="gpill on",
                          **{"data-val":"tous"}),
            ]+[
                html.Span(g, id=f"gpill-{i}", n_clicks=0,
                          className="gpill",
                          **{"data-val":g})
                for i,g in enumerate(GROUPES)
            ], className="grp-pills"),
        ]),
        html.Div([
            html.Div("🏦 Banques", className="fl"),
            dcc.Dropdown(id="fb",
                options=[{"label":b,"value":b} for b in BANQUES],
                value=BANQUES_PDF[:8], multi=True,
                style={"width":"280px","fontSize":"11px"}),
        ]),
        dcc.Store(id="fg-store", data="tous"),
    ], id="fbar"),

    # KPIs
    html.Div([
        html.Div([
            html.Div(style={"height":"3px","background":acc,
                            "borderRadius":"12px 12px 0 0",
                            "margin":"-14px -16px 0",
                            "marginBottom":"0"}, className="kc-bar"),
            html.Span(ico, className="ki"),
            html.Div(lbl, className="kl"),
            html.Div(id=kid, className="kv",
                     style={"color":acc}),
        ], className="kc")
        for kid,lbl,acc,ico in KPI_CONF
    ], id="kpi-row"),

    dcc.Store(id="active-tab", data="t1"),

    # Content
    html.Div([

        # T1 — Bilan
        html.Div([
            dbc.Row([gc("Classement par Bilan total","g1",sub="M FCFA"),
                     gc("Emplois vs Ressources","g2")], className="g-3 mb-3"),
            dbc.Row([gc("Fonds propres","g3"),
                     gc("Répartition par groupe","g4")], className="g-3"),
        ], id="tab-t1", className="tab-pg on"),

        # T2 — Finance
        html.Div([
            dbc.Row([gc("Produit Net Bancaire","g5",sub="M FCFA"),
                     gc("Résultat Net","g6")], className="g-3 mb-3"),
            dbc.Row([gc("Coefficient d'exploitation — seuil 60%","g7",12)], className="g-3"),
        ], id="tab-t2", className="tab-pg"),

        # T3 — Ratios
        html.Div([
            dbc.Row([gc("ROA — Rentabilité des actifs","g8"),
                     gc("ROE — Rentabilité fonds propres","g9")], className="g-3 mb-3"),
            dbc.Row([gc("Solvabilité — FP/Bilan","g10"),
                     gc("Scatter ROE vs Solvabilité","g11")], className="g-3"),
        ], id="tab-t3", className="tab-pg"),

        # T4 — Evolution
        html.Div([
            dbc.Row([gc("Évolution du Bilan","g12"),
                     gc("Évolution du Résultat Net","g13")], className="g-3 mb-3"),
            dbc.Row([gc("Évolution du ROA","g14",12)], className="g-3"),
        ], id="tab-t4", className="tab-pg"),

        # T5 — Carte
        html.Div([
            dbc.Row([dbc.Col([
                html.Div("Indicateur", className="fl",
                         style={"marginTop":"4px","marginBottom":"6px"}),
                dcc.Dropdown(id="fc", clearable=False,
                    options=[{"label":"Bilan","value":"bilan"},
                             {"label":"Emplois","value":"emploi"},
                             {"label":"Fonds propres","value":"fonds_propre"},
                             {"label":"Ressources","value":"ressources"},
                             {"label":"PNB","value":"produit_net_bancaire"},
                             {"label":"Résultat net","value":"resultat_net"}],
                    value="bilan", style={"fontSize":"11px"}),
            ], width=4)], className="mb-3"),
            html.Div([
                html.Div([html.Span(className="gcd"),
                          "📍 Banques à Dakar"], className="gct"),
                dcc.Graph(id="g15", config={"displayModeBar":False}),
            ], className="gc"),
        ], id="tab-t5", className="tab-pg"),

        # T6 — Comparaison
        html.Div([
            dbc.Row([gc("Top 10 — Bilan","g16"),
                     gc("Top 10 — Résultat Net","g17")], className="g-3 mb-3"),
            dbc.Row([gc("Radar — Comparaison multidimensionnelle","g18",12)], className="g-3"),
        ], id="tab-t6", className="tab-pg"),

        # T7 — Tableau
        html.Div([
            html.Div([
                html.Div([html.Span(className="gcd"),
                          "Tous les indicateurs — filtrable & triable"], className="gct"),
                dash_table.DataTable(id="tbl",
                    style_table={"overflowX":"auto"},
                    style_header={"textAlign":"center"},
                    style_cell={"textAlign":"center","minWidth":"70px"},
                    page_size=15, sort_action="native", filter_action="native"),
            ], className="gc"),
        ], id="tab-t7", className="tab-pg"),

    ], id="content"),

])

# ── Clientside callback — navigation ─────────────────────────────────────────
app.clientside_callback(
    """
    function(at) {
        ['t1','t2','t3','t4','t5','t6','t7'].forEach(function(t){
            var pg = document.getElementById('tab-' + t);
            if(pg) pg.className = 'tab-pg' + (t===at?' on':'');
        });
        return window.dash_clientside.no_update;
    }
    """,
    Output("active-tab","id"),
    Input("active-tab","data"),
)

@app.callback(
    Output("active-tab","data"),
    [Input(f"nav-{tid}","n_clicks") for tid,_,_ in TABS],
    prevent_initial_call=False,
)
def switch_tab(*args):
    ctx = dash.callback_context
    if not ctx.triggered or not ctx.triggered[0]["value"]:
        return "t1"
    tid = ctx.triggered[0]["prop_id"].split(".")[0].replace("nav-","")
    return tid

@app.callback(
    Output("yr-disp","children"),
    Input("fa","value"),
)
def update_yr(a): return str(int(a)) if a else ""

# ── Helpers ───────────────────────────────────────────────────────────────────
def fil(annee, groupe, banques):
    d = df[df["annee"]==annee].copy()
    if groupe != "tous": d = d[d["groupe_bancaire"]==groupe]
    if banques: d = d[d["sigle"].isin(banques)]
    if d.empty:
        d = df[df["annee"]==annee].copy()
    return d

def fmv(v):
    if v is None or (isinstance(v,float) and np.isnan(v)): return "—"
    return f"{v/1e6:.2f} Mds"

def efig(h=320):
    fig = go.Figure()
    fig.add_annotation(text="Aucune donnée",xref="paper",yref="paper",
                       x=0.5,y=0.5,showarrow=False,
                       font=dict(size=13,color="#94a3b8"))
    fig.update_layout(height=h,paper_bgcolor="white",plot_bgcolor="white",
                      margin=dict(l=5,r=5,t=5,b=5))
    return fig

def bl(fig, h=320):
    fig.update_layout(
        height=h, margin=dict(l=5,r=5,t=10,b=5),
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(color="#475569",family="Inter, sans-serif",size=11),
        legend=dict(orientation="h",y=1.14,font_size=10,
                    bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(gridcolor="#f1f5f9",linecolor="#e2e8f0",
                   tickcolor="#cbd5e1",tickfont=dict(size=10,color="#64748b")),
        yaxis=dict(gridcolor="#f1f5f9",linecolor="#e2e8f0",
                   tickcolor="#cbd5e1",tickfont=dict(size=10,color="#64748b")),
    )
    return fig

# ── KPIs ──────────────────────────────────────────────────────────────────────
@app.callback(
    Output("k-b","children"),Output("k-e","children"),Output("k-r","children"),
    Output("k-f","children"),Output("k-roa","children"),Output("k-p","children"),
    Input("fa","value"),Input("fg-store","data"),Input("fb","value"),
)
def kpis(a,g,b):
    d = fil(a,g,b)
    if d.empty: return "—","—","—","—","—","—"
    return (
        fmv(d["bilan"].sum()), fmv(d["emploi"].sum()),
        fmv(d["ressources"].sum()), fmv(d["fonds_propre"].mean()),
        f"{d['ratio_roa'].mean():.2f}%" if d["ratio_roa"].notna().any() else "—",
        fmv(d["produit_net_bancaire"].mean()),
    )

# ── T1 ────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("g1","figure"),Output("g2","figure"),
    Output("g3","figure"),Output("g4","figure"),
    Input("fa","value"),Input("fg-store","data"),Input("fb","value"),
)
def tab_bilan(a,g,b):
    d = fil(a,g,b)
    if d.empty: return efig(),efig(),efig(),efig()
    d1 = d.dropna(subset=["bilan"]).sort_values("bilan")
    f1 = bl(px.bar(d1,x="bilan",y="sigle",orientation="h",
                   color="groupe_bancaire",color_discrete_sequence=PALETTE,
                   labels={"bilan":"Bilan (M FCFA)","sigle":""}))
    d2 = d.dropna(subset=["emploi","ressources"])
    f2 = go.Figure([
        go.Bar(name="Emplois",x=d2["sigle"],y=d2["emploi"],marker_color="#2563eb",marker_opacity=0.85),
        go.Bar(name="Ressources",x=d2["sigle"],y=d2["ressources"],marker_color="#0891b2",marker_opacity=0.85),
    ])
    f2.update_layout(barmode="group"); bl(f2)
    d3 = d.dropna(subset=["fonds_propre"]).sort_values("fonds_propre",ascending=False)
    f3 = bl(px.bar(d3,x="sigle",y="fonds_propre",color="groupe_bancaire",
                   color_discrete_sequence=PALETTE,
                   labels={"fonds_propre":"Fonds propres (M FCFA)","sigle":""}))
    pt = d.groupby("groupe_bancaire")["bilan"].sum().reset_index()
    f4 = px.pie(pt,values="bilan",names="groupe_bancaire",hole=0.5,
                color_discrete_sequence=PALETTE)
    f4.update_traces(textposition="inside",textinfo="percent+label",
                     marker=dict(line=dict(color="white",width=2)))
    f4.update_layout(height=320,margin=dict(l=5,r=5,t=5,b=5),
                     paper_bgcolor="white",font=dict(color="#475569",size=11),
                     legend=dict(font_size=10))
    return f1,f2,f3,f4

# ── T2 ────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("g5","figure"),Output("g6","figure"),Output("g7","figure"),
    Input("fa","value"),Input("fg-store","data"),Input("fb","value"),
)
def tab_finance(a,g,b):
    d = fil(a,g,b)
    if d.empty: return efig(),efig(),efig()
    d1 = d.dropna(subset=["produit_net_bancaire"]).sort_values("produit_net_bancaire",ascending=False)
    f1 = bl(px.bar(d1,x="sigle",y="produit_net_bancaire",color="groupe_bancaire",
                   color_discrete_sequence=PALETTE,
                   labels={"produit_net_bancaire":"PNB (M FCFA)","sigle":""}))
    d2 = d.dropna(subset=["resultat_net"]).sort_values("resultat_net",ascending=False)
    f2 = bl(px.bar(d2,x="sigle",y="resultat_net",color="groupe_bancaire",
                   color_discrete_sequence=PALETTE,
                   labels={"resultat_net":"Résultat net","sigle":""}))
    f2.add_hline(y=0,line_dash="dash",line_color="#94a3b8",line_width=1)
    d3 = d.dropna(subset=["ratio_exploitation"]).sort_values("ratio_exploitation")
    f3 = bl(px.bar(d3,x="sigle",y="ratio_exploitation",color="groupe_bancaire",
                   color_discrete_sequence=PALETTE,
                   labels={"ratio_exploitation":"Coeff. (%)","sigle":""}))
    f3.add_hline(y=60,line_dash="dash",line_color="#dc2626",line_width=1.5,
                 annotation_text="Seuil 60%",annotation_position="top right",
                 annotation_font_color="#dc2626",annotation_font_size=11)
    return f1,f2,f3

# ── T3 ────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("g8","figure"),Output("g9","figure"),
    Output("g10","figure"),Output("g11","figure"),
    Input("fa","value"),Input("fg-store","data"),Input("fb","value"),
)
def tab_ratios(a,g,b):
    d = fil(a,g,b)
    if d.empty: return efig(),efig(),efig(),efig()
    def bar_r(col,lbl):
        dd = d.dropna(subset=[col]).sort_values(col,ascending=False)
        if dd.empty: return efig()
        f = bl(px.bar(dd,x="sigle",y=col,color="groupe_bancaire",
                      color_discrete_sequence=PALETTE,labels={col:lbl,"sigle":""}))
        f.add_hline(y=0,line_dash="dash",line_color="#94a3b8",line_width=1)
        return f
    f1=bar_r("ratio_roa","ROA (%)")
    f2=bar_r("ratio_roe","ROE (%)")
    f3=bar_r("ratio_solvabilite","Solvabilité (%)")
    d4 = d.dropna(subset=["ratio_roe","ratio_solvabilite","bilan"])
    if d4.empty: return f1,f2,f3,efig()
    f4 = bl(px.scatter(d4,x="ratio_solvabilite",y="ratio_roe",
                       size="bilan",color="groupe_bancaire",text="sigle",
                       color_discrete_sequence=PALETTE,
                       labels={"ratio_solvabilite":"Solvabilité (%)","ratio_roe":"ROE (%)"}))
    f4.update_traces(textposition="top center",textfont=dict(size=10,color="#475569"))
    return f1,f2,f3,f4

# ── T4 ────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("g12","figure"),Output("g13","figure"),Output("g14","figure"),
    Input("fg-store","data"),Input("fb","value"),
)
def tab_evol(g,b):
    d = df.copy()
    if g != "tous": d = d[d["groupe_bancaire"]==g]
    if b: d = d[d["sigle"].isin(b)]
    if d.empty: return efig(),efig(),efig()
    def ln(col,lbl):
        dd = d.dropna(subset=[col])
        if dd.empty: return efig()
        f = bl(px.line(dd,x="annee",y=col,color="sigle",markers=True,
                       color_discrete_sequence=PALETTE,
                       labels={col:lbl,"annee":"Année"}))
        f.update_traces(line=dict(width=2),marker=dict(size=6))
        return f
    return ln("bilan","Bilan"),ln("resultat_net","Résultat net"),ln("ratio_roa","ROA (%)")

# ── T5 ────────────────────────────────────────────────────────────────────────
@app.callback(Output("g15","figure"),Input("fa","value"),Input("fc","value"))
def carte(a,indic):
    rows = []
    for s,(lat,lon) in COORDS.items():
        sub = df[(df["sigle"]==s)&(df["annee"]==a)]
        if sub.empty: continue
        val = sub.iloc[0].get(indic,0) or 0
        rows.append({"sigle":s,"lat":lat,"lon":lon,"valeur":val,
                     "groupe":sub.iloc[0].get("groupe_bancaire","")})
    dfc = pd.DataFrame(rows)
    if dfc.empty: return efig(430)
    fig = px.scatter_mapbox(dfc,lat="lat",lon="lon",text="sigle",
                            size="valeur",color="groupe",size_max=45,zoom=11,
                            center={"lat":14.6928,"lon":-17.4467},
                            mapbox_style="open-street-map",
                            color_discrete_sequence=PALETTE)
    fig.update_layout(margin=dict(l=0,r=0,t=0,b=0),height=430,
                      paper_bgcolor="white",
                      legend=dict(orientation="h",y=-0.06,font_size=10))
    return fig

# ── T6 ────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("g16","figure"),Output("g17","figure"),Output("g18","figure"),
    Input("fa","value"),Input("fg-store","data"),Input("fb","value"),
)
def tab_comp(a,g,b):
    d = fil(a,g,b)
    if d.empty: return efig(320),efig(320),efig(340)
    d1 = d.dropna(subset=["bilan"]).sort_values("bilan",ascending=False).head(10)
    f1 = bl(px.bar(d1,x="bilan",y="sigle",orientation="h",
                   color="groupe_bancaire",color_discrete_sequence=PALETTE,
                   labels={"bilan":"Bilan (M FCFA)","sigle":""}),320)
    d2 = d.dropna(subset=["resultat_net"]).sort_values("resultat_net",ascending=False).head(10)
    f2 = bl(px.bar(d2,x="resultat_net",y="sigle",orientation="h",
                   color="groupe_bancaire",color_discrete_sequence=PALETTE,
                   labels={"resultat_net":"Résultat net","sigle":""}),320)
    f2.add_vline(x=0,line_dash="dash",line_color="#94a3b8",line_width=1)
    cols_r = ["ratio_roa","ratio_roe","ratio_solvabilite","ratio_exploitation","ratio_marge_nette"]
    d3 = d.dropna(subset=cols_r)
    if d3.empty: return f1,f2,efig(340)
    dn = d3.copy()
    for c in cols_r:
        mn,mx = dn[c].min(),dn[c].max()
        dn[c] = (dn[c]-mn)/(mx-mn)*100 if mx!=mn else 50
    labs = ["ROA","ROE","Solvabilité","Coeff Exploit.","Marge nette"]
    f3 = go.Figure()
    for i,(_,row) in enumerate(dn.head(6).iterrows()):
        vs = [row[c] for c in cols_r]+[row[cols_r[0]]]
        f3.add_trace(go.Scatterpolar(r=vs,theta=labs+[labs[0]],
                                     name=row["sigle"],fill="toself",
                                     opacity=0.5,line_color=PALETTE[i%len(PALETTE)]))
    f3.update_layout(
        polar=dict(radialaxis=dict(visible=True,range=[0,100],
                                  gridcolor="#e2e8f0",linecolor="#cbd5e1",
                                  tickfont=dict(color="#94a3b8",size=9)),
                   angularaxis=dict(gridcolor="#e2e8f0",linecolor="#cbd5e1",
                                   tickfont=dict(color="#475569",size=10)),
                   bgcolor="white"),
        height=340,margin=dict(l=40,r=40,t=20,b=20),
        paper_bgcolor="white",
        font=dict(color="#475569",family="Inter, sans-serif"),
        legend=dict(orientation="h",y=-0.15,font_size=10,bgcolor="rgba(0,0,0,0)"),
    )
    return f1,f2,f3

# ── T7 ────────────────────────────────────────────────────────────────────────
@app.callback(
    Output("tbl","data"),Output("tbl","columns"),
    Input("fa","value"),Input("fg-store","data"),Input("fb","value"),
)
def tableau(a,g,b):
    d = fil(a,g,b)
    cols = ["sigle","groupe_bancaire","annee","bilan","emploi","ressources",
            "fonds_propre","effectif","agence","compte","produit_net_bancaire",
            "charges_generales_exploitation","resultat_brut_exploitation",
            "cout_du_risque","resultat_net","ratio_roa","ratio_roe",
            "ratio_solvabilite","ratio_exploitation","ratio_marge_nette"]
    ok = [c for c in cols if c in d.columns]
    d = d[ok].round(2)
    return d.to_dict("records"),[{"name":c.replace("_"," ").upper(),"id":c} for c in ok]

# ── PDF ───────────────────────────────────────────────────────────────────────
@app.callback(Output("dl-pdf","data"),
              Input("btn-pdf","n_clicks"),
              State("pdf-b","value"),State("fa","value"),
              prevent_initial_call=True)
def pdf_dl(n,b,a):
    data = generer_pdf(b,a)
    if not data: return dash.no_update
    return dcc.send_bytes(data,filename=f"rapport_{b}_{a}.pdf")

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀  Dashboard → http://127.0.0.1:8050")
    app.run(debug=True)
    server = app.server