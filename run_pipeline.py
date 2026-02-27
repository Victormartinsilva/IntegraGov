#!/usr/bin/env python3
"""
Pipeline principal do IntegraGov - Fase 1 (MVP Saúde e Demografia).
Execução local: coleta IBGE (municípios, população) + PySUS (SIM, Sinasc) -> Bronze -> Silver -> Gold.
"""
import sys
import logging
from pathlib import Path

import pandas as pd

# Garante que o projeto esteja no path ao rodar localmente
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import BRONZE_DIR, FASE_MVP
from src.connectors.ibge import IBGEConnector
from src.connectors.datasus import DatasusConnector
from src.transform.silver import SilverTransform
from src.transform.gold import GoldTransform
from src.db import ensure_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("integragov")


def run_fase1(ano: int = 2024, amostra_municipios: list[int] | None = None, todos_municipios: bool = False, amostra_n: int | None = None):
    """
    Fase 1: MVP Saúde e Demografia.
    - Coleta municípios e população (IBGE)
    - Coleta dados de saúde via PySUS (SIM, Sinasc) se instalado
    - Bronze -> Silver -> Gold
    """
    logger.info("Iniciando pipeline IntegraGov - Fase 1 (ano=%s, todos_municipios=%s)", ano, todos_municipios)

    ensure_schema()

    # --- Bronze: extração ---
    ibge = IBGEConnector(bronze_dir=BRONZE_DIR)
    df_mun = ibge.listar_municipios()
    if df_mun.empty:
        logger.error("Falha ao obter municípios do IBGE.")
        return
    ibge.salvar_bronze_municipios(df_mun)

    # População: todos (API N6all), amostra customizada, ou amostra por tamanho (padrão 100)
    AMOSTRA_PADRAO = 100
    n_amostra = amostra_n if amostra_n is not None else AMOSTRA_PADRAO
    codigos = None
    if todos_municipios:
        codigos = None  # consulta todos
    elif amostra_municipios is not None and len(amostra_municipios) > 0:
        codigos = [int(x) for x in amostra_municipios]
    elif len(df_mun) > n_amostra:
        codigos = df_mun["cod_mun_ibge_7"].astype(int).head(n_amostra).tolist()
        logger.info("Usando amostra de %d municípios para população (use --amostra N ou --todos-municipios).", len(codigos))

    df_pop = ibge.obter_populacao_municipios(ano=ano, codigos_municipios=codigos)
    if df_pop.empty:
        logger.warning("Nenhum dado de população retornado; tentando ano anterior.")
        df_pop = ibge.obter_populacao_municipios(ano=ano - 1, codigos_municipios=codigos)
    if not df_pop.empty:
        ibge.salvar_bronze_populacao(df_pop, ano)

    # Dados de saúde: via PySUS (FTP DATASUS). APIs REST do Ministério da Saúde estão fora do ar (404).
    datasus = DatasusConnector(bronze_dir=BRONZE_DIR)
    df_obitos = datasus.sim_obitos(ano=ano)
    if not df_obitos.empty:
        datasus.salvar_bronze(df_obitos, "sim_obitos")
        logger.info("SIM (óbitos): %d municípios via PySUS.", len(df_obitos))
    else:
        logger.info("SIM (óbitos) não disponível. Instale PySUS para incluir: pip install pysus")
    df_nascidos = datasus.sinasc_nascidos_vivos(ano=ano)
    if not df_nascidos.empty:
        datasus.salvar_bronze(df_nascidos, "sinasc_nascidos_vivos")
        logger.info("Sinasc (nascidos vivos): %d municípios via PySUS.", len(df_nascidos))
    else:
        logger.info("Sinasc não disponível. Instale PySUS para incluir: pip install pysus")

    # --- Silver: padronização ---
    silver = SilverTransform()
    df_mun_s = silver.bronze_ibge_municipios_para_silver()
    df_pop_s = silver.bronze_ibge_populacao_para_silver(ano=ano)
    if df_pop_s.empty and not df_pop.empty:
        df_pop_s = silver.padronizar_codigo_municipio(df_pop.copy())

    # Padronizar código município em DATASUS se houver coluna compatível
    df_obitos_s = pd.DataFrame()
    if not df_obitos.empty:
        for c in ["CODMUN", "codigo_ibge", "cod_mun_ibge_7", "codmun"]:
            if c in df_obitos.columns:
                df_obitos = df_obitos.rename(columns={c: "cod_mun_ibge_7"})
                break
        if "cod_mun_ibge_7" not in df_obitos.columns and "municipio" in df_obitos.columns:
            # Pode ser necessário join com dim_municipio depois
            pass
        df_obitos_s = silver.padronizar_codigo_municipio(df_obitos)
    df_nascidos_s = pd.DataFrame()
    if not df_nascidos.empty:
        for c in ["CODMUN", "codigo_ibge", "cod_mun_ibge_7", "codmun"]:
            if c in df_nascidos.columns:
                df_nascidos = df_nascidos.rename(columns={c: "cod_mun_ibge_7"})
                break
        df_nascidos_s = silver.padronizar_codigo_municipio(df_nascidos)

    silver.persistir_silver_no_banco(df_mun_s, df_pop_s, None)
    silver.salvar_silver_parquet(df_mun_s, "municipios")
    if not df_pop_s.empty:
        silver.salvar_silver_parquet(df_pop_s, "populacao")

    # --- Gold: indicadores ---
    gold_t = GoldTransform()
    df_gold = gold_t.indicadores_saude_por_municipio(
        df_populacao=df_pop_s,
        df_obitos=df_obitos_s if not df_obitos_s.empty else None,
        df_nascidos=df_nascidos_s if not df_nascidos_s.empty else None,
        ano=ano,
    )
    if not df_gold.empty:
        gold_t.persistir_gold_no_banco(df_gold)
        path_gold = gold_t.salvar_gold_parquet(df_gold)
        logger.info("Pipeline concluído. Gold salvo em: %s", path_gold)
    else:
        logger.warning("Gold vazio (sem população Silver). Verifique Bronze IBGE.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="IntegraGov - Pipeline Fase 1 (Saúde e Demografia)")
    parser.add_argument("--ano", type=int, default=2024, help="Ano de referência")
    parser.add_argument("--amostra", type=int, default=None, metavar="N", help="Número de municípios na amostra (padrão: 100)")
    parser.add_argument("--todos-municipios", action="store_true", help="Consultar todos os municípios (pode ser lento)")
    args = parser.parse_args()
    run_fase1(ano=args.ano, todos_municipios=args.todos_municipios, amostra_n=args.amostra)
