#!/usr/bin/env python3
"""
IntegraGov - Interface para visualização das tabelas Gold.
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

from config.settings import DB_PATH, DATA_DIR, GOLD_DIR

# Mapa (Folium) — importação condicional para não quebrar se não instalado
try:
    import folium
    from streamlit_folium import st_folium
    FOLIUM_AVAILABLE = True
except ImportError:
    FOLIUM_AVAILABLE = False

st.set_page_config(
    page_title="IntegraGov - Dados Gold",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Estilo simples e legível
st.markdown("""
<style>
    .stDataFrame { font-size: 0.9em; }
    div[data-testid="stMetricValue"] { font-size: 1.4rem; }
    .tabela-info { padding: 0.5rem 1rem; background: #f0f2f6; border-radius: 0.5rem; margin: 0.5rem 0; }
</style>
""", unsafe_allow_html=True)


def get_connection():
    """Conexão com o banco. Nova a cada execução (Streamlit roda em threads diferentes por rerun)."""
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(str(DB_PATH))


def listar_tabelas_gold(conn) -> list[str]:
    """Retorna nomes das tabelas que começam com 'gold_'."""
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'gold_%' ORDER BY name"
    )
    return [row[0] for row in cur.fetchall()]


def carregar_tabela(conn, nome_tabela: str) -> pd.DataFrame:
    """Carrega tabela como DataFrame."""
    return pd.read_sql(f"SELECT * FROM [{nome_tabela}]", conn)


def carregar_gold_com_municipio(conn) -> pd.DataFrame:
    """Gold indicadores saúde com nome do município (join com dim_municipio)."""
    sql = """
    SELECT 
        g.cod_mun_ibge_7,
        m.nome_municipio,
        m.sigla_uf,
        g.ano,
        g.populacao,
        g.total_internacoes,
        g.total_obitos,
        g.nascidos_vivos,
        g.taxa_internacao_100k,
        g.taxa_obitos_100k,
        g.data_carga
    FROM gold_indicadores_saude_municipio g
    LEFT JOIN dim_municipio m ON m.cod_mun_ibge_7 = g.cod_mun_ibge_7
    ORDER BY g.populacao DESC
    """
    return pd.read_sql(sql, conn)


CENTROIDES_URL = (
    "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv"
)


@st.cache_data(ttl=3600)
def carregar_centroides_municipios() -> pd.DataFrame:
    """Carrega lat/lon dos municípios (código IBGE 7 dígitos). Fonte: Municipios-Brasileiros/kelvins."""
    try:
        r = requests.get(CENTROIDES_URL, timeout=15)
        r.raise_for_status()
        df = pd.read_csv(io.BytesIO(r.content), sep=",")
        df["cod_mun_ibge_7"] = df["codigo_ibge"].astype(str).str.zfill(7)
        return df[["cod_mun_ibge_7", "latitude", "longitude", "nome"]].drop_duplicates("cod_mun_ibge_7")
    except Exception:
        return pd.DataFrame()


def main():
    st.title("📊 IntegraGov – Visualização dos Dados Gold")
    st.caption("Sistema de Integração de Dados Públicos | Camada Gold (analítica)")

    conn = get_connection()
    if conn is None:
        st.warning(
            f"Banco de dados não encontrado em `{DB_PATH}`. "
            "Execute o pipeline primeiro: `python run_pipeline.py --ano 2024`"
        )
        return

    # Sidebar: escolha da tabela / visão
    tabelas_gold = listar_tabelas_gold(conn)
    if not tabelas_gold:
        st.warning("Nenhuma tabela Gold encontrada no banco. Execute o pipeline.")
        conn.close()
        return

    st.sidebar.header("Tabelas Gold")
    tem_indicadores = "gold_indicadores_saude_municipio" in tabelas_gold
    opcoes = []
    if tem_indicadores:
        opcoes.append("Indicadores de saúde por município (com nomes)")
    opcoes.extend([t for t in tabelas_gold if t != "gold_indicadores_saude_municipio"])
    if tem_indicadores:
        opcoes.append("gold_indicadores_saude_municipio (bruto)")

    tabela_escolhida = st.sidebar.radio(
        "Selecione a tabela ou visão",
        options=opcoes,
        index=0,
    )

    # Conteúdo principal: visão enriquecida ou tabela bruta
    if "com nomes" in tabela_escolhida:
        df = carregar_gold_com_municipio(conn)
        titulo = "Indicadores de Saúde por Município (Gold)"
    else:
        nome_tabela = tabela_escolhida.split("(")[0].strip() if "(" in tabela_escolhida else tabela_escolhida
        df = carregar_tabela(conn, nome_tabela)
        titulo = f"Tabela: {nome_tabela}"

    st.subheader(titulo)

    # Aviso quando óbitos/nascidos estão zerados (API DATASUS pode ter retornado 404)
    if not df.empty and "total_obitos" in df.columns and "nascidos_vivos" in df.columns:
        if df["total_obitos"].fillna(0).sum() == 0 and df["nascidos_vivos"].fillna(0).sum() == 0:
            st.info(
                "**Óbitos e nascidos vivos estão zerados.** Para incluir esses dados, instale o PySUS e rode o pipeline de novo: "
                "`pip install pysus` e depois `python run_pipeline.py --ano 2024`. O PySUS baixa os dados do FTP do DATASUS."
            )

    # Filtros (quando fizer sentido)
    if not df.empty and "sigla_uf" in df.columns:
        ufs = ["Todos"] + sorted(df["sigla_uf"].dropna().unique().tolist())
        uf = st.sidebar.selectbox("Filtrar por UF", ufs)
        if uf != "Todos":
            df = df[df["sigla_uf"] == uf]
    if not df.empty and "ano" in df.columns:
        anos = ["Todos"] + sorted(df["ano"].dropna().unique().astype(int).tolist(), reverse=True)
        ano = st.sidebar.selectbox("Filtrar por ano", anos)
        if ano != "Todos":
            df = df[df["ano"] == int(ano)]

    # Mapa do Brasil + selectbox Município (apenas na visão "Indicadores com nomes")
    df_with_coords = pd.DataFrame()
    if (
        "com nomes" in tabela_escolhida
        and not df.empty
        and "cod_mun_ibge_7" in df.columns
        and "nome_municipio" in df.columns
        and "sigla_uf" in df.columns
    ):
        centroides = carregar_centroides_municipios()
        if not centroides.empty:
            df_with_coords = df.merge(
                centroides[["cod_mun_ibge_7", "latitude", "longitude"]],
                on="cod_mun_ibge_7",
                how="left",
            )
        df_unique = df[["cod_mun_ibge_7", "nome_municipio", "sigla_uf"]].drop_duplicates()
        opcoes_mun = ["Todos"] + sorted(
            [f"{r['nome_municipio']} ({r['sigla_uf']})" for _, r in df_unique.iterrows()],
            key=str.lower,
        )
        municipio_sel = st.sidebar.selectbox(
            "📍 Município (ir para tabela filtrada)",
            opcoes_mun,
            index=0,
            help="Selecione um município para filtrar a tabela e centralizar o mapa.",
        )
        if municipio_sel != "Todos":
            mask = (
                (df_unique["nome_municipio"] + " (" + df_unique["sigla_uf"] + ")") == municipio_sel
            )
            cod_sel = df_unique.loc[mask, "cod_mun_ibge_7"].iloc[0]
            df = df[df["cod_mun_ibge_7"] == cod_sel]
            if not df_with_coords.empty:
                df_with_coords = df_with_coords[df_with_coords["cod_mun_ibge_7"] == cod_sel]

        # Desenhar mapa (Folium)
        if not FOLIUM_AVAILABLE and not df.empty:
            st.info("Instale `folium` e `streamlit-folium` para exibir o mapa: `pip install folium streamlit-folium`")
        elif FOLIUM_AVAILABLE and not df_with_coords.empty and "latitude" in df_with_coords.columns:
            # Um ponto por município (evitar duplicata por ano)
            df_map = (
                df_with_coords.dropna(subset=["latitude", "longitude"])
                .drop_duplicates(subset=["cod_mun_ibge_7"])
            )
            if not df_map.empty:
                if len(df_map) == 1:
                    lat_c, lon_c = df_map.iloc[0]["latitude"], df_map.iloc[0]["longitude"]
                    zoom = 9
                else:
                    lat_c, lon_c = -14.5, -51.5
                    zoom = 4
                mapa = folium.Map(location=[lat_c, lon_c], zoom_start=zoom, tiles="OpenStreetMap")
                for _, row in df_map.iterrows():
                    pop = int(row.get("populacao", 0) or 0)
                    radius = min(30, max(6, (pop / 1e6) * 20)) if pop else 8
                    tooltip = (
                        f"<b>{row.get('nome_municipio', '')} ({row.get('sigla_uf', '')})</b><br>"
                        f"População: {pop:,}<br>"
                        f"Óbitos: {int(row.get('total_obitos', 0) or 0):,}<br>"
                        f"Nascidos vivos: {int(row.get('nascidos_vivos', 0) or 0):,}"
                    )
                    folium.CircleMarker(
                        location=[row["latitude"], row["longitude"]],
                        radius=radius,
                        color="#2563eb",
                        fill=True,
                        fillColor="#3b82f6",
                        fillOpacity=0.6,
                        tooltip=folium.Tooltip(tooltip, sticky=True),
                    ).add_to(mapa)
                st.subheader("🗺️ Mapa — municípios com dados")
                st_folium(mapa, width="stretch", height=400)
                st.caption("Bolinhas indicam municípios com dados na base. Selecione um município na barra lateral para filtrar.")

    # Métricas (após todos os filtros: UF, ano, município)
    n_rows, n_cols = len(df), len(df.columns)
    col1, col2, col3 = st.columns(3)
    col1.metric("Registros", f"{n_rows:,}")
    col2.metric("Colunas", n_cols)
    if not df.empty and "data_carga" in df.columns and not df["data_carga"].empty:
        ultima = pd.to_datetime(df["data_carga"].dropna(), errors="coerce").max()
        col3.metric("Última carga", str(ultima)[:19] if pd.notna(ultima) else "—")
    else:
        col3.metric("Última carga", "—")

    config = {}
    for col, label, fmt in [
        ("populacao", "População", "%d"),
        ("total_internacoes", "Internações", "%d"),
        ("total_obitos", "Óbitos", "%d"),
        ("nascidos_vivos", "Nascidos vivos", "%d"),
        ("taxa_internacao_100k", "Taxa intern. (por 100k)", "%.2f"),
        ("taxa_obitos_100k", "Taxa óbitos (por 100k)", "%.2f"),
    ]:
        if col in df.columns:
            config[col] = st.column_config.NumberColumn(label, format=fmt)

    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        column_config=config if config else None,
    )

    # Download
    st.sidebar.divider()
    csv = df.to_csv(index=False, sep=";", encoding="utf-8-sig")
    st.sidebar.download_button(
        "📥 Exportar CSV",
        data=csv,
        file_name="integragov_gold_export.csv",
        mime="text/csv",
    )

    # Resumo das tabelas no banco
    st.sidebar.divider()
    st.sidebar.caption("Tabelas Gold no banco")
    for t in tabelas_gold:
        n = pd.read_sql(f"SELECT COUNT(*) as n FROM [{t}]", conn).iloc[0]["n"]
        st.sidebar.text(f"  • {t}: {n:,} linhas")

    conn.close()


if __name__ == "__main__":
    main()
