#!/usr/bin/env python3
"""
IntegraGov - Dashboard de Dados Públicos Brasileiros
Execute: streamlit run app.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import io
import sqlite3
import pandas as pd
import streamlit as st
import requests

from config.settings import DB_PATH

try:
    import folium
    from streamlit_folium import st_folium
    FOLIUM_AVAILABLE = True
except ImportError:
    FOLIUM_AVAILABLE = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

st.set_page_config(
    page_title="IntegraGov – Dados Públicos",
    page_icon="🇧🇷",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Estilo
# ---------------------------------------------------------------------------
st.markdown("""
<style>
  .stApp { background: linear-gradient(160deg, #0F1626 0%, #131c2e 100%); }
  [data-testid="stHeader"] { background: rgba(13,19,33,0.97); border-bottom: 1px solid #1c2b40; }
  [data-testid="stSidebar"] { background: #0b1120; border-right: 1px solid #1c2b40; }
  h1, h2, h3 { color: #47E0E0 !important; font-weight: 700; letter-spacing: -0.5px; }
  .stMarkdown p { color: #c0cfe0; }
  div[data-testid="stMetricValue"] { color: #47E0E0 !important; font-size: 1.6rem !important; font-weight: 700; }
  div[data-testid="stMetricLabel"] { color: #6b829e; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.6px; }
  [data-testid="stDataFrame"] { border: 1px solid #1c2b40; border-radius: 10px; }
  .stAlert { background: #131e30; border-left: 4px solid #47E0E0; color: #c0cfe0; }
  .stTabs [data-baseweb="tab-list"] { background: #0b1120; border-bottom: 2px solid #1c2b40; gap: 2px; padding: 0 8px; }
  .stTabs [data-baseweb="tab"] { color: #6b829e; border-radius: 6px 6px 0 0; padding: 10px 22px; font-weight: 500; font-size: 0.9rem; }
  .stTabs [aria-selected="true"] { color: #47E0E0 !important; background: #111d30 !important; border-bottom: 2px solid #47E0E0 !important; }
  div[data-testid="stSidebarContent"] hr { border-color: #1c2b40; }
  .fonte-badge {
    background: #0d1e30; border: 1px solid #1c3a50; border-radius: 4px;
    padding: 3px 10px; font-size: 0.75rem; color: #47E0E0; display: inline-block; margin: 2px;
  }
  .info-card {
    background: #111d30; border: 1px solid #1c2b40; border-radius: 10px;
    padding: 14px 18px; margin-bottom: 8px;
  }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
CENTROIDES_URL = (
    "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv"
)
COR_CYAN = "#47E0E0"
COR_VERDE = "#68d391"
COR_AZUL = "#63b3ed"
COR_ROXO = "#b794f4"
COR_AMARELO = "#f6e05e"
COR_VERMELHO = "#fc8181"
COR_LARANJA = "#f6ad55"
PLOTLY_TMPL = "plotly_dark"
PLOTLY_BG = "rgba(0,0,0,0)"
PLOTLY_PAPER = "rgba(0,0,0,0)"
PLOTLY_FONT = "#c0cfe0"


def _plotly_layout(height: int = 360, **kwargs) -> dict:
    base = dict(
        height=height,
        plot_bgcolor=PLOTLY_BG,
        paper_bgcolor=PLOTLY_PAPER,
        font_color=PLOTLY_FONT,
        margin=dict(l=0, r=10, t=40, b=0),
        showlegend=False,
        xaxis_title="",
        yaxis_title="",
    )
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# Carregamento de dados (com cache)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _load_sql(db_path: str, sql: str) -> pd.DataFrame:
    try:
        with sqlite3.connect(db_path) as conn:
            return pd.read_sql(sql, conn)
    except Exception:
        return pd.DataFrame()


def load_saude(db_path: str) -> pd.DataFrame:
    return _load_sql(db_path, """
        SELECT g.cod_mun_ibge_7, m.nome_municipio, m.sigla_uf, g.ano,
               g.populacao, g.total_internacoes, g.total_obitos, g.nascidos_vivos,
               g.taxa_internacao_100k, g.taxa_obitos_100k, g.data_carga
        FROM gold_indicadores_saude_municipio g
        LEFT JOIN dim_municipio m ON m.cod_mun_ibge_7 = g.cod_mun_ibge_7
        ORDER BY g.populacao DESC
    """)


def load_educacao(db_path: str) -> pd.DataFrame:
    return _load_sql(db_path, """
        SELECT g.cod_mun_ibge_7, m.nome_municipio, m.sigla_uf, g.ano,
               g.matriculas, g.docentes, g.escolas, g.taxa_matriculas_por_1000_hab, g.data_carga
        FROM gold_indicadores_educacao_municipio g
        LEFT JOIN dim_municipio m ON m.cod_mun_ibge_7 = g.cod_mun_ibge_7
        ORDER BY g.matriculas DESC
    """)


def load_pib(db_path: str) -> pd.DataFrame:
    try:
        with sqlite3.connect(db_path) as conn:
            tabelas = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            if "gold_pib_municipio" not in tabelas:
                return pd.DataFrame()
        return _load_sql(db_path, """
            SELECT g.cod_mun_ibge_7, m.nome_municipio, m.sigla_uf, g.ano,
                   g.pib_total_mil_reais, g.pib_per_capita, g.data_carga
            FROM gold_pib_municipio g
            LEFT JOIN dim_municipio m ON m.cod_mun_ibge_7 = g.cod_mun_ibge_7
            ORDER BY g.pib_total_mil_reais DESC
        """)
    except Exception:
        return pd.DataFrame()


def load_transferencias(db_path: str) -> pd.DataFrame:
    try:
        with sqlite3.connect(db_path) as conn:
            tabelas = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            if "gold_transparencia_transferencias" not in tabelas:
                return pd.DataFrame()
        return _load_sql(db_path, """
            SELECT g.cod_mun_ibge_7, m.nome_municipio, m.sigla_uf, g.ano,
                   g.total_transferencias_reais, g.data_carga
            FROM gold_transparencia_transferencias g
            LEFT JOIN dim_municipio m ON m.cod_mun_ibge_7 = g.cod_mun_ibge_7
            ORDER BY g.total_transferencias_reais DESC
        """)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_centroides() -> pd.DataFrame:
    try:
        r = requests.get(CENTROIDES_URL, timeout=15)
        r.raise_for_status()
        df = pd.read_csv(io.BytesIO(r.content))
        df["cod_mun_ibge_7"] = df["codigo_ibge"].astype(str).str.zfill(7)
        return df[["cod_mun_ibge_7", "latitude", "longitude"]].drop_duplicates("cod_mun_ibge_7")
    except Exception:
        return pd.DataFrame()


def apply_filters(df: pd.DataFrame, uf: str, ano) -> pd.DataFrame:
    if df.empty:
        return df
    if uf != "Todos" and "sigla_uf" in df.columns:
        df = df[df["sigla_uf"] == uf]
    if ano != "Todos" and "ano" in df.columns:
        df = df[df["ano"] == int(ano)]
    return df


# ---------------------------------------------------------------------------
# Gráficos
# ---------------------------------------------------------------------------

def _chart_top_bar(df: pd.DataFrame, col: str, label_col: str, title: str, color: str, n: int = 10, fmt: str = ",.0f") -> None:
    if not PLOTLY_AVAILABLE:
        st.caption("Instale `plotly` para ver gráficos: `pip install plotly`")
        return
    if df.empty or col not in df.columns:
        st.caption("Sem dados suficientes para o gráfico.")
        return
    top = df.nlargest(n, col)[[label_col, col]].dropna()
    if top.empty:
        st.caption("Sem dados suficientes.")
        return
    fig = px.bar(
        top.sort_values(col),
        x=col, y=label_col,
        orientation="h",
        title=title,
        color_discrete_sequence=[color],
        template=PLOTLY_TMPL,
        text=col,
    )
    fig.update_traces(texttemplate=f"%{{text:{fmt}}}", textposition="outside", marker_line_width=0)
    fig.update_layout(**_plotly_layout())
    st.plotly_chart(fig, width="stretch")


def _chart_scatter(df: pd.DataFrame, x: str, y: str, hover: str, title: str, color: str, xlabel: str = "", ylabel: str = "") -> None:
    if not PLOTLY_AVAILABLE or df.empty or x not in df.columns or y not in df.columns:
        return
    fig = px.scatter(
        df.dropna(subset=[x, y]),
        x=x, y=y,
        hover_name=hover if hover in df.columns else None,
        title=title,
        color_discrete_sequence=[color],
        template=PLOTLY_TMPL,
        opacity=0.65,
    )
    fig.update_layout(**_plotly_layout(xaxis_title=xlabel, yaxis_title=ylabel))
    st.plotly_chart(fig, width="stretch")


def _chart_bar_uf(df: pd.DataFrame, col: str, title: str, color: str) -> None:
    if not PLOTLY_AVAILABLE or df.empty or "sigla_uf" not in df.columns or col not in df.columns:
        return
    agg = df.groupby("sigla_uf")[col].sum().reset_index().sort_values(col, ascending=False)
    fig = px.bar(
        agg, x="sigla_uf", y=col,
        title=title,
        color_discrete_sequence=[color],
        template=PLOTLY_TMPL,
        text=col,
    )
    fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside", marker_line_width=0)
    fig.update_layout(**_plotly_layout())
    st.plotly_chart(fig, width="stretch")


# ---------------------------------------------------------------------------
# Abas
# ---------------------------------------------------------------------------

def _kpi_row(kpis: list[tuple]) -> None:
    """Renderiza linha de KPIs. kpis = [(label, value, delta_opt)]"""
    cols = st.columns(len(kpis))
    for i, item in enumerate(kpis):
        label, value = item[0], item[1]
        delta = item[2] if len(item) > 2 else None
        if delta:
            cols[i].metric(label, value, delta)
        else:
            cols[i].metric(label, value)


def tab_visao_geral(df_s: pd.DataFrame, df_e: pd.DataFrame, df_p: pd.DataFrame, df_t: pd.DataFrame) -> None:
    n_mun = df_s["cod_mun_ibge_7"].nunique() if not df_s.empty else 0
    n_ufs = df_s["sigla_uf"].nunique() if not df_s.empty else 0
    pop = int(df_s.groupby("cod_mun_ibge_7")["populacao"].max().sum()) if not df_s.empty and "populacao" in df_s.columns else 0
    mat = int(df_e["matriculas"].fillna(0).sum()) if not df_e.empty else 0

    _kpi_row([
        ("Municípios com dados", f"{n_mun:,}"),
        ("Estados (UF)", str(n_ufs)),
        ("População total", f"{pop:,}"),
        ("Matrículas escolares", f"{mat:,}"),
    ])

    st.divider()

    if not df_s.empty:
        col1, col2 = st.columns(2)
        with col1:
            _chart_top_bar(df_s, "populacao", "nome_municipio", "🏙️ Top 10 municípios por população", COR_CYAN)
        with col2:
            _chart_bar_uf(df_s, "populacao", "📍 População por UF", COR_AZUL)

    st.divider()
    st.markdown("### Dados disponíveis por tema")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown("**🏥 Saúde**")
        if not df_s.empty:
            ob = int(df_s["total_obitos"].fillna(0).sum()) if "total_obitos" in df_s.columns else 0
            nv = int(df_s["nascidos_vivos"].fillna(0).sum()) if "nascidos_vivos" in df_s.columns else 0
            st.metric("Óbitos", f"{ob:,}")
            st.metric("Nascidos vivos", f"{nv:,}")
            if ob == 0:
                st.caption("⚠️ Instale `pysus` para óbitos/nascimentos")
        else:
            st.caption("Execute o pipeline para carregar dados.")
    with c2:
        st.markdown("**📚 Educação**")
        if not df_e.empty:
            doc = int(df_e["docentes"].fillna(0).sum()) if "docentes" in df_e.columns else 0
            esc = int(df_e["escolas"].fillna(0).sum()) if "escolas" in df_e.columns else 0
            st.metric("Docentes", f"{doc:,}")
            st.metric("Escolas", f"{esc:,}")
        else:
            st.caption("Sem dados de educação.")
    with c3:
        st.markdown("**💰 PIB Municipal**")
        if not df_p.empty and "pib_total_mil_reais" in df_p.columns:
            pib = df_p["pib_total_mil_reais"].sum()
            pc = df_p["pib_per_capita"].median() if "pib_per_capita" in df_p.columns else None
            st.metric("PIB total (R$ mil)", f"{int(pib):,}")
            if pc:
                st.metric("PIB per capita mediano", f"R$ {pc:,.0f}")
        else:
            st.caption("Execute com `--incluir-pib` para dados de PIB.")
    with c4:
        st.markdown("**🏛️ Transferências**")
        if not df_t.empty and "total_transferencias_reais" in df_t.columns:
            total = df_t["total_transferencias_reais"].sum()
            st.metric("Total transferido", f"R$ {total/1e9:.1f} bi")
        else:
            st.caption("Execute com `--incluir-transparencia` e configure `TRANSPARENCIA_API_KEY`.")

    st.divider()
    st.markdown("**Fontes de dados integradas:**")
    fontes = {
        "IBGE Localidades": "servicodados.ibge.gov.br",
        "IBGE SIDRA (Pop.)": "sidra.ibge.gov.br",
        "IBGE SIDRA (PIB)": "sidra.ibge.gov.br",
        "DATASUS/PySUS": "datasus.saude.gov.br",
        "INEP Censo Escolar": "inep.gov.br",
        "Portal da Transparência": "portaldatransparencia.gov.br",
    }
    badges = " ".join(f'<span class="fonte-badge">{f}</span>' for f in fontes)
    st.markdown(badges, unsafe_allow_html=True)


def tab_saude(df_s: pd.DataFrame) -> None:
    if df_s.empty:
        st.info("Sem dados de saúde. Execute: `python run_pipeline.py --ano 2024`")
        return

    ob = int(df_s["total_obitos"].fillna(0).sum()) if "total_obitos" in df_s.columns else 0
    nv = int(df_s["nascidos_vivos"].fillna(0).sum()) if "nascidos_vivos" in df_s.columns else 0
    n_mun = df_s["cod_mun_ibge_7"].nunique()
    pop = int(df_s.groupby("cod_mun_ibge_7")["populacao"].max().sum()) if "populacao" in df_s.columns else 0

    _kpi_row([
        ("Municípios", f"{n_mun:,}"),
        ("População", f"{pop:,}"),
        ("Óbitos registrados", f"{ob:,}"),
        ("Nascidos vivos", f"{nv:,}"),
    ])

    sem_saude = ob == 0 and nv == 0
    if sem_saude:
        st.warning("⚠️ Óbitos e nascidos vivos zerados. Instale PySUS: `pip install pysus` e reexecute o pipeline.")
        st.info("O DATASUS disponibiliza esses dados via FTP. O PySUS facilita o acesso.")

    st.divider()
    if not sem_saude:
        col1, col2 = st.columns(2)
        with col1:
            _chart_top_bar(df_s, "taxa_obitos_100k", "nome_municipio", "💀 Taxa de óbitos (por 100k hab.) – Top 10", COR_VERMELHO, fmt=",.1f")
        with col2:
            _chart_top_bar(df_s, "nascidos_vivos", "nome_municipio", "👶 Nascidos vivos – Top 10", COR_VERDE)
        st.divider()

    col1, col2 = st.columns(2)
    with col1:
        _chart_top_bar(df_s, "populacao", "nome_municipio", "👥 Maiores populações", COR_AZUL)
    with col2:
        if not sem_saude:
            _chart_scatter(df_s, "populacao", "total_obitos", "nome_municipio",
                           "Relação: População × Óbitos", COR_CYAN,
                           xlabel="População", ylabel="Óbitos")


def tab_educacao(df_e: pd.DataFrame) -> None:
    if df_e.empty:
        st.info("Sem dados de educação (INEP Censo Escolar). Execute o pipeline.")
        st.markdown("""
        O **Censo Escolar** é realizado anualmente pelo INEP e contém dados de matrículas,
        docentes e escolas por município. O pipeline tenta baixar os dados automaticamente,
        mas o arquivo ZIP do INEP pode ser grande (centenas de MB).
        """)
        return

    mat = int(df_e["matriculas"].fillna(0).sum()) if "matriculas" in df_e.columns else 0
    doc = int(df_e["docentes"].fillna(0).sum()) if "docentes" in df_e.columns else 0
    esc = int(df_e["escolas"].fillna(0).sum()) if "escolas" in df_e.columns else 0
    n_mun = df_e["cod_mun_ibge_7"].nunique()

    _kpi_row([
        ("Municípios", f"{n_mun:,}"),
        ("Matrículas", f"{mat:,}"),
        ("Docentes", f"{doc:,}"),
        ("Escolas", f"{esc:,}"),
    ])

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        _chart_top_bar(df_e, "matriculas", "nome_municipio", "📚 Municípios com mais matrículas – Top 10", COR_AZUL)
    with col2:
        _chart_top_bar(df_e, "taxa_matriculas_por_1000_hab", "nome_municipio",
                       "📊 Taxa de matrículas (por 1.000 hab.) – Top 10", COR_ROXO, fmt=",.1f")

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        _chart_top_bar(df_e, "escolas", "nome_municipio", "🏫 Municípios com mais escolas – Top 10", COR_VERDE)
    with col2:
        _chart_scatter(df_e, "escolas", "docentes", "nome_municipio",
                       "Relação: Escolas × Docentes por município", COR_ROXO,
                       xlabel="Escolas", ylabel="Docentes")


def tab_pib(df_p: pd.DataFrame) -> None:
    if df_p.empty:
        st.info(
            "Sem dados de PIB municipal. Execute com a flag `--incluir-pib`:\n\n"
            "`python run_pipeline.py --ano 2024 --incluir-pib`\n\n"
            "O PIB municipal é extraído da API pública IBGE SIDRA (tabela 5938). "
            "Os dados são publicados com ~2 anos de defasagem (último disponível: 2021)."
        )
        return

    pib_total = df_p["pib_total_mil_reais"].sum() if "pib_total_mil_reais" in df_p.columns else 0
    pib_pc_med = df_p["pib_per_capita"].median() if "pib_per_capita" in df_p.columns else 0
    pib_pc_max = df_p["pib_per_capita"].max() if "pib_per_capita" in df_p.columns else 0
    n_mun = df_p["cod_mun_ibge_7"].nunique()

    _kpi_row([
        ("Municípios com PIB", f"{n_mun:,}"),
        ("PIB total (R$ bilhões)", f"{pib_total/1_000_000:.1f}"),
        ("PIB per capita mediano", f"R$ {pib_pc_med:,.0f}"),
        ("PIB per capita máximo", f"R$ {pib_pc_max:,.0f}"),
    ])

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        _chart_top_bar(df_p, "pib_total_mil_reais", "nome_municipio", "💰 Maiores PIBs municipais (R$ mil) – Top 10", COR_VERDE)
    with col2:
        _chart_top_bar(df_p, "pib_per_capita", "nome_municipio", "💵 Maior PIB per capita (R$) – Top 10", COR_AMARELO, fmt=",.0f")

    st.divider()
    _chart_scatter(df_p, "pib_total_mil_reais", "pib_per_capita", "nome_municipio",
                   "PIB total × PIB per capita", COR_VERDE,
                   xlabel="PIB total (R$ mil)", ylabel="PIB per capita (R$)")


def tab_mapa(df_s: pd.DataFrame, df_e: pd.DataFrame, df_p: pd.DataFrame, centroides: pd.DataFrame) -> None:
    if not FOLIUM_AVAILABLE:
        st.info("Instale folium para ver o mapa: `pip install folium streamlit-folium`")
        return
    if centroides.empty:
        st.warning("Não foi possível carregar os centróides dos municípios (verifique conexão).")
        return

    views_disp = []
    if not df_s.empty:
        views_disp.append("🏥 Saúde")
    if not df_e.empty:
        views_disp.append("📚 Educação")
    if not df_p.empty:
        views_disp.append("💰 PIB Municipal")

    if not views_disp:
        st.info("Sem dados para exibir no mapa. Execute o pipeline primeiro.")
        return

    col_sel, col_ind = st.columns([2, 3])
    with col_sel:
        view_sel = st.radio("Camada de dados", views_disp, horizontal=False)
    with col_ind:
        if "Saúde" in view_sel:
            df_map_base = df_s
            opcoes_ind = [
                ("População", "populacao"),
                ("Óbitos", "total_obitos"),
                ("Nascidos vivos", "nascidos_vivos"),
                ("Taxa de óbitos (por 100k)", "taxa_obitos_100k"),
            ]
        elif "Educação" in view_sel:
            df_map_base = df_e
            opcoes_ind = [
                ("Matrículas", "matriculas"),
                ("Docentes", "docentes"),
                ("Escolas", "escolas"),
                ("Taxa matrículas (por 1k hab.)", "taxa_matriculas_por_1000_hab"),
            ]
        else:
            df_map_base = df_p
            opcoes_ind = [
                ("PIB total (R$ mil)", "pib_total_mil_reais"),
                ("PIB per capita (R$)", "pib_per_capita"),
            ]
        opcoes_disp = [(l, c) for l, c in opcoes_ind if c in df_map_base.columns]
        if not opcoes_disp:
            st.info("Nenhum indicador disponível.")
            return
        ind_label = st.selectbox("Indicador (tamanho do círculo)", [l for l, _ in opcoes_disp])
        ind_col = next(c for l, c in opcoes_disp if l == ind_label)

    df_joined = df_map_base.merge(centroides, on="cod_mun_ibge_7", how="inner").dropna(subset=["latitude", "longitude"])
    if df_joined.empty:
        st.info("Sem coordenadas para os municípios selecionados.")
        return

    # Agregar: um ponto por município
    agg_cols = {"latitude": "first", "longitude": "first"}
    for c in ["nome_municipio", "sigla_uf"] + [col for _, col in opcoes_disp]:
        if c in df_joined.columns:
            agg_cols[c] = "first"
    df_mapa = df_joined.groupby("cod_mun_ibge_7", as_index=False).agg(agg_cols)

    serie = df_mapa[ind_col].fillna(0)
    max_v = serie.max()
    escala = 22.0 / max_v if max_v > 0 else 0.01

    zoom = 9 if len(df_mapa) == 1 else 4
    lat_c = float(df_mapa.iloc[0]["latitude"]) if len(df_mapa) == 1 else -14.5
    lon_c = float(df_mapa.iloc[0]["longitude"]) if len(df_mapa) == 1 else -51.5

    mapa = folium.Map(
        location=[lat_c, lon_c],
        zoom_start=zoom,
        tiles="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; CARTO',
        control_scale=True,
    )

    for _, row in df_mapa.iterrows():
        val = float(row.get(ind_col) or 0)
        radius = min(28, max(5, val * escala)) if val else 5
        nome = row.get("nome_municipio", "")
        uf = row.get("sigla_uf", "")
        linhas = [f"<b style='color:#47E0E0'>{nome} ({uf})</b>"]
        for lbl, col in opcoes_disp:
            v = row.get(col)
            if v is not None and not pd.isna(v):
                fv = f"{float(v):,.0f}" if float(v) == int(float(v)) else f"{float(v):,.2f}"
                linhas.append(f"{lbl}: {fv}")
        tooltip_html = "<br>".join(linhas)
        loc = [float(row["latitude"]), float(row["longitude"])]
        folium.CircleMarker(loc, radius=radius + 8, color=COR_CYAN, fill=True,
                            fillColor=COR_CYAN, fillOpacity=0.1, weight=0).add_to(mapa)
        folium.CircleMarker(loc, radius=radius, color=COR_CYAN, fill=True,
                            fillColor=COR_CYAN, fillOpacity=0.75, weight=1,
                            tooltip=folium.Tooltip(tooltip_html, sticky=True)).add_to(mapa)

    st_folium(mapa, use_container_width=True, height=520)
    st.caption(f"**Legenda:** o tamanho do círculo representa **{ind_label}**. Passe o mouse sobre um município para detalhes.")


def tab_dados_brutos(db_path: str, uf: str, ano) -> None:
    try:
        with sqlite3.connect(db_path) as conn:
            tabelas = [
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                ).fetchall()
            ]
    except Exception:
        st.error("Erro ao acessar o banco de dados.")
        return

    if not tabelas:
        st.info("Nenhuma tabela encontrada no banco.")
        return

    tabela = st.selectbox("Tabela", tabelas)
    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql(f"SELECT * FROM [{tabela}]", conn)
    except Exception as e:
        st.error(f"Erro ao carregar `{tabela}`: {e}")
        return

    if "sigla_uf" in df.columns and uf != "Todos":
        df = df[df["sigla_uf"] == uf]
    if "ano" in df.columns and ano != "Todos":
        df = df[df["ano"] == int(ano)]

    st.caption(f"{len(df):,} registros · {len(df.columns)} colunas")

    col_cfg = {}
    for col, label, fmt in [
        ("populacao", "População", "%d"),
        ("total_internacoes", "Internações", "%d"),
        ("total_obitos", "Óbitos", "%d"),
        ("nascidos_vivos", "Nascidos vivos", "%d"),
        ("taxa_internacao_100k", "Taxa intern. (100k)", "%.2f"),
        ("taxa_obitos_100k", "Taxa óbitos (100k)", "%.2f"),
        ("matriculas", "Matrículas", "%d"),
        ("docentes", "Docentes", "%d"),
        ("escolas", "Escolas", "%d"),
        ("pib_total_mil_reais", "PIB (R$ mil)", "%.0f"),
        ("pib_per_capita", "PIB per capita (R$)", "%.0f"),
        ("total_transferencias_reais", "Transferências (R$)", "%.0f"),
    ]:
        if col in df.columns:
            col_cfg[col] = st.column_config.NumberColumn(label, format=fmt)

    st.dataframe(df, width="stretch", hide_index=True, column_config=col_cfg or None)

    csv = df.to_csv(index=False, sep=";", encoding="utf-8-sig")
    st.download_button(
        "📥 Exportar CSV",
        data=csv,
        file_name=f"integragov_{tabela}.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # Header
    st.markdown("""
    <div style="display:flex;align-items:center;gap:14px;padding:4px 0 12px 0">
      <span style="font-size:2.4rem;line-height:1">🇧🇷</span>
      <div>
        <div style="font-size:1.9rem;font-weight:800;color:#47E0E0;letter-spacing:-1px;line-height:1.1">IntegraGov</div>
        <div style="font-size:0.82rem;color:#6b829e;letter-spacing:0.3px">
          Integração de dados públicos brasileiros · IBGE · DATASUS · INEP · Transparência
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    if not DB_PATH.exists():
        st.warning(
            "Banco de dados não encontrado. Execute o pipeline:\n\n"
            "`python run_pipeline.py --ano 2024`"
        )
        with st.expander("ℹ️ Como começar"):
            st.markdown("""
            1. Instale as dependências: `pip install -r requirements.txt`
            2. Execute o pipeline: `python run_pipeline.py --ano 2024`
            3. Para incluir PIB: `python run_pipeline.py --ano 2024 --incluir-pib`
            4. Para incluir transferências federais: configure `TRANSPARENCIA_API_KEY` e use `--incluir-transparencia`
            """)
        return

    db_path = str(DB_PATH)

    # Carregar dados
    with st.spinner("Carregando dados..."):
        df_s = load_saude(db_path)
        df_e = load_educacao(db_path)
        df_p = load_pib(db_path)
        df_t = load_transferencias(db_path)
        centroides = load_centroides()

    # Sidebar: filtros
    st.sidebar.markdown("## 🔍 Filtros")

    ufs_disp: set = set()
    for df in [df_s, df_e, df_p]:
        if not df.empty and "sigla_uf" in df.columns:
            ufs_disp.update(df["sigla_uf"].dropna().unique())
    uf = "Todos"
    if ufs_disp:
        uf = st.sidebar.selectbox("Estado (UF)", ["Todos"] + sorted(ufs_disp))

    anos_disp: set = set()
    for df in [df_s, df_e, df_p]:
        if not df.empty and "ano" in df.columns:
            anos_disp.update(df["ano"].dropna().astype(int).unique())
    ano = "Todos"
    if anos_disp:
        ano = st.sidebar.selectbox("Ano", ["Todos"] + sorted(anos_disp, reverse=True))

    # Aplicar filtros
    df_s_f = apply_filters(df_s, uf, ano)
    df_e_f = apply_filters(df_e, uf, ano)
    df_p_f = apply_filters(df_p, uf, ano)
    df_t_f = apply_filters(df_t, uf, ano)

    # Sidebar: status das fontes
    st.sidebar.divider()
    st.sidebar.markdown("### 📡 Status das fontes")
    status = [
        ("IBGE (municípios/pop.)", not df_s.empty),
        ("DATASUS (saúde)", not df_s.empty and (df_s.get("total_obitos", pd.Series([0])).sum() > 0 if not df_s.empty else False)),
        ("INEP (educação)", not df_e.empty),
        ("IBGE (PIB)", not df_p.empty),
        ("Transparência", not df_t.empty),
    ]
    for fonte, ok in status:
        icon = "✅" if ok else "⚪"
        st.sidebar.markdown(f"{icon} {fonte}")

    if df_s.empty and df_e.empty:
        st.sidebar.error("Sem dados. Execute `python run_pipeline.py --ano 2024`")

    st.sidebar.divider()
    with st.sidebar.expander("ℹ️ Sobre o IntegraGov"):
        st.markdown("""
        **IntegraGov** integra dados públicos abertos do governo brasileiro em uma única interface analítica.

        **Fontes:**
        - **IBGE** – municípios, população (SIDRA), PIB municipal (SIDRA 5938)
        - **DATASUS/PySUS** – óbitos (SIM) e nascidos vivos (Sinasc)
        - **INEP** – Censo Escolar (matrículas, docentes, escolas)
        - **Portal da Transparência** – transferências constitucionais (FPM, FUNDEB…)

        **Arquitetura:** Data Lakehouse (Bronze → Silver → Gold) · SQLite local
        """)

    # Abas
    tabs = st.tabs([
        "📊 Visão Geral",
        "🏥 Saúde",
        "📚 Educação",
        "💰 PIB Municipal",
        "🗺️ Mapa",
        "📋 Dados brutos",
    ])

    with tabs[0]:
        tab_visao_geral(df_s_f, df_e_f, df_p_f, df_t_f)
    with tabs[1]:
        tab_saude(df_s_f)
    with tabs[2]:
        tab_educacao(df_e_f)
    with tabs[3]:
        tab_pib(df_p_f)
    with tabs[4]:
        tab_mapa(df_s_f, df_e_f, df_p_f, centroides)
    with tabs[5]:
        tab_dados_brutos(db_path, uf, ano)


if __name__ == "__main__":
    main()
